import pandas as pd
import os
import requests
import time
import re  # Added for Regex parsing
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
print("--- Starting Data Processing Script ---")
load_dotenv(dotenv_path='../.env')

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY") 

DATA_DIR = '../data/'
# You can add more districts here if needed
TARGET_DISTRICTS = ['Kanpur Dehat', 'Kanpur Nagar', 'Jalaun', 'Auraiya', 'Lucknow', 'Unnao']

# --- FILE PATHS ---
NHA_INPUT = os.path.join(DATA_DIR, 'nha_hospitals_raw.csv')
NABH_INPUT = os.path.join(DATA_DIR, 'nabh_hospitals_raw.txt') # Changed to .txt
CGHS_INPUT = os.path.join(DATA_DIR, 'cghs_rates_raw.csv')
KB_INPUT = os.path.join(DATA_DIR, 'knowledge_base.csv')

HOSPITALS_OUTPUT = os.path.join(DATA_DIR, 'hospitals_processed.csv')
COSTS_OUTPUT = os.path.join(DATA_DIR, 'costs_processed.csv')


# --- 2. HELPER FUNCTIONS ---

def clean_hospital_name(name):
    """Standardizes hospital names for better matching."""
    if not isinstance(name, str): return ''
    name = name.lower()
    # Remove specific noisy words to increase match chances
    replacements = ['hospital', 'research', 'centre', 'center', 'pvt', 'ltd', 'private', 'limited', '&', 'and', 'institute', 'medical', 'sciences']
    for word in replacements:
        name = name.replace(word, '')
    # Remove all whitespace/punctuation to match "CityHospital" with "City Hospital"
    return ''.join(e for e in name if e.isalnum())

def parse_nabh_text_file(file_path):
    """
    Parses the messy NABH text dump using Regex to extract hospital names.
    """
    print("  Parsing raw NABH text file...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Regex Logic:
        # The text pattern seems to be: [Number] [H-Code] [Hospital Name, City, State] [H-Code]
        # We look for text between two H-20xx codes.
        pattern = r"H-\d{4}-\d{4}(.*?)H-\d{4}-"
        matches = re.findall(pattern, content)
        
        extracted_names = []
        for match in matches:
            # The match includes "Name, City, State, India". 
            # We usually just want the name, which is before the first comma.
            # However, some names have commas (e.g., "Care Hospital, Unit 1").
            # We will keep the whole string as it helps with matching.
            extracted_names.append(match.strip())
            
        print(f"  Extracted {len(extracted_names)} accredited hospitals from text.")
        return pd.DataFrame(extracted_names, columns=['Hospital Name'])
        
    except Exception as e:
        print(f"  Error reading NABH text file: {e}")
        return pd.DataFrame(columns=['Hospital Name'])

def get_google_maps_data(hospital_name, address, district, api_key):
    """Calls Google Places API to get rating and location."""
    # Construct a search query: "Hospital Name District State"
    search_query = f"{hospital_name} {district}"
    
    url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={search_query}&inputtype=textquery&fields=rating,user_ratings_total,geometry&key={api_key}"
    try:
        response = requests.get(url)
        data = response.json()
        if data['status'] == 'OK' and data.get('candidates'):
            candidate = data['candidates'][0]
            rating = candidate.get('rating', 3.5) 
            total_ratings = candidate.get('user_ratings_total', 0)
            location = candidate['geometry']['location']
            return rating, total_ratings, location['lat'], location['lng']
    except Exception as e:
        print(f"  Error fetching Google data for {hospital_name}: {e}")
    return 3.5, 0, None, None 

# --- 3. MAIN PROCESSING LOGIC ---

def create_datasets():
    # --- PROCESS HOSPITALS ---
    print("\n[Step 1/3] Processing Hospital Data...")
    
    # 1. Load NHA Data (Master List)
    try:
        nha_df = pd.read_csv(NHA_INPUT)
        # Rename columns to standard names if they differ in your CSV
        # Assuming standard columns: 'Hospital Name', 'Address', 'District Name'
    except FileNotFoundError:
        print("Error: nha_hospitals_raw.csv not found.")
        return

    # 2. Load NABH Data (Quality List) using the special parser
    nabh_df = parse_nabh_text_file(NABH_INPUT)
    
    # 3. Filter NHA list for target districts
    # Note: Check your CSV column headers. They might be 'Hospital Name' or 'Hosp Name'
    hospitals_df = nha_df[nha_df['District Name'].isin(TARGET_DISTRICTS)].copy()
    print(f"  Filtered to {len(hospitals_df)} hospitals in target districts.")

    # 4. Create clean names for fuzzy matching
    hospitals_df['clean_name'] = hospitals_df['Hospital Name'].apply(clean_hospital_name)
    nabh_df['clean_name'] = nabh_df['Hospital Name'].apply(clean_hospital_name)
    
    # Create a set for fast lookup
    nabh_clean_names = set(nabh_df['clean_name'])

    # 5. Cross-reference: Is the hospital in the NABH list?
    hospitals_df['is_nabh_accredited'] = hospitals_df['clean_name'].isin(nabh_clean_names)
    print(f"  Matched {hospitals_df['is_nabh_accredited'].sum()} NABH-accredited hospitals.")

    # 6. Enrich with Google Maps Data
    if not API_KEY or API_KEY == "YOUR_KEY":
        print("  WARNING: No Google Maps API Key found. Skipping Google Data fetch.")
        hospitals_df['google_rating'] = 3.5
        hospitals_df['google_ratings_total'] = 0
        hospitals_df['latitude'] = 26.4499 # Default (Kanpur)
        hospitals_df['longitude'] = 80.3319
    else:
        print("  Fetching Google Maps data (this may take time)...")
        google_data = []
        for index, row in hospitals_df.iterrows():
            rating, total, lat, lon = get_google_maps_data(row['Hospital Name'], row['Address'], row['District Name'], API_KEY)
            google_data.append({
                'google_rating': rating, 
                'google_ratings_total': total, 
                'latitude': lat, 
                'longitude': lon
            })
            # time.sleep(0.1) # Tiny pause to be safe
        
        google_df = pd.DataFrame(google_data, index=hospitals_df.index)
        hospitals_df = pd.concat([hospitals_df, google_df], axis=1)

    # 7. Assign Tiers
    hospitals_df['hospital_tier'] = 'C'
    # Tier B: Good rating and decent review count
    hospitals_df.loc[(hospitals_df['google_rating'] > 4.0) & (hospitals_df['google_ratings_total'] > 50), 'hospital_tier'] = 'B'
    # Tier A: NABH Accredited (Overrides others)
    hospitals_df.loc[hospitals_df['is_nabh_accredited'], 'hospital_tier'] = 'A'

    # 8. Save Final Hospital File
    final_hospitals_df = hospitals_df[[
        'Hospital Name', 'Address', 'District Name', 'is_nabh_accredited', 'hospital_tier',
        'google_rating', 'google_ratings_total', 'latitude', 'longitude'
    ]].rename(columns={'Hospital Name': 'name', 'Address': 'address', 'District Name': 'district'})
    
    final_hospitals_df.to_csv(HOSPITALS_OUTPUT, index=False)
    print(f"  SUCCESS: Processed hospital data saved.")

    # --- PROCESS COSTS ---
    print("\n[Step 2/3] Processing Cost Data...")
    try:
        cghs_df = pd.read_csv(CGHS_INPUT) 
        kb_df = pd.read_csv(KB_INPUT)     
        
        # Merge Knowledge Base with CGHS Rates
        # Note: Ensure 'disease_name_english' in KB matches 'procedure_name' in CGHS exactly
        disease_cost_map = kb_df.merge(cghs_df, left_on='disease_name_english', right_on='procedure_name', how='inner')
        
        all_costs = []
        
        # Reload the hospital file we just saved to get IDs
        processed_hospitals_df = pd.read_csv(HOSPITALS_OUTPUT)
        # Add a temporary ID column based on index (1, 2, 3...)
        processed_hospitals_df['hospital_id'] = processed_hospitals_df.index + 1

        for _, hospital in processed_hospitals_df.iterrows():
            for _, treatment in disease_cost_map.iterrows():
                base_cost = treatment['rate']
                
                # Dynamic Multiplier Logic
                tier_multipliers = {'A': 1.6, 'B': 1.2, 'C': 1.0}
                base_mult = tier_multipliers.get(hospital['hospital_tier'], 1.0)
                
                # Rating Adjustment: +/- 5% based on star rating
                rating_adj = (hospital['google_rating'] - 4.0) / 20.0 
                final_mult = base_mult + rating_adj
                
                estimated_cost = int(base_cost * final_mult)
                
                all_costs.append({
                    'hospital_id': hospital['hospital_id'],
                    'treatment_id': treatment['treatment_id'],
                    'estimated_cost': estimated_cost
                })

        costs_df = pd.DataFrame(all_costs)
        costs_df.to_csv(COSTS_OUTPUT, index=False)
        print(f"  SUCCESS: Dynamic cost data saved.")
        
        # Save hospitals again with the ID column for the database
        processed_hospitals_df.to_csv(HOSPITALS_OUTPUT, index=False)
        
    except Exception as e:
        print(f"  Error processing costs: {e}")

    print("\n[Step 3/3] All processing complete!")

# --- 4. RUN SCRIPT ---
if __name__ == '__main__':
    if not API_KEY or API_KEY == "YOUR_GOOGLE_MAPS_API_KEY_HERE":
        print("!!! WARNING: GOOGLE_MAPS_API_KEY is missing in .env. Location data will be dummy values.")
    create_datasets()
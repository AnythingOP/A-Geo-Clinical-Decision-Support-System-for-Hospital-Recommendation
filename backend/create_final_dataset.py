import pandas as pd
import os
import re
import time
import random
from geopy.geocoders import Nominatim

print("--- Starting FINAL Data Processing (Demo Mode) ---")
DATA = '../data/'

# Input Files
NHA = os.path.join(DATA, 'nha_hospitals_raw.csv')
NABH = os.path.join(DATA, 'nabh_hospitals_raw.txt')
CGHS = os.path.join(DATA, 'cghs_rates_raw.csv')
KB = os.path.join(DATA, 'knowledge_base.csv')

# Output Files
OUT_H = os.path.join(DATA, 'hospitals_processed.csv')
OUT_C = os.path.join(DATA, 'costs_processed.csv')

# Free Geocoder
geolocator = Nominatim(user_agent="aarogya_demo_v5")

def clean(name):
    if not isinstance(name, str): return ''
    name = name.lower()
    for w in ['hospital','research','centre','center','pvt','ltd','private','limited']:
        name = name.replace(w, '')
    return ''.join(e for e in name if e.isalnum())

def get_geo(name, district, state):
    try:
        # Try specific search
        q = f"{name}, {district}, {state}"
        loc = geolocator.geocode(q, timeout=2)
        if loc: return loc.latitude, loc.longitude
        
        # Fallback to district
        loc = geolocator.geocode(f"{district}, {state}", timeout=2)
        if loc: return loc.latitude, loc.longitude
    except: pass
    # Default Fallback (Kanpur Center)
    return 26.4499, 80.3319

def run():
    # --- 1. LOAD HOSPITALS ---
    try:
        # Robust Read: Try Tab first, then Comma
        df = pd.read_csv(NHA, sep='\t', on_bad_lines='skip', engine='python')
        if len(df.columns) < 5:
            df = pd.read_csv(NHA, sep=',', on_bad_lines='skip', engine='python')
        
        col_map = {'Hospital Name': 'name', 'District': 'District Name', 'City Name': 'District Name', 'State': 'State Name'}
        df.rename(columns=col_map, inplace=True)
        
        # Generate Address if missing
        if 'address' not in df.columns:
            df['address'] = df['name'] + ", " + df.get('District Name', '') + ", " + df.get('State Name', '')
            
        print(f"  Loaded {len(df)} hospitals from source.")
    except Exception as e:
        return print(f"  Error reading NHA file: {e}")

    # --- 2. NABH CHECK ---
    try:
        with open(NABH, 'r', encoding='utf-8') as f: txt = f.read()
        nabh_names = [m.split(',')[0].strip() for m in re.findall(r"H-\d{4}-\d{4}(.*?)H-\d{4}-", txt)]
        nabh_set = set([clean(n) for n in nabh_names])
        df['clean'] = df['name'].apply(clean)
        df['is_nabh_accredited'] = df['clean'].isin(nabh_set)
    except:
        df['is_nabh_accredited'] = False

    # --- 3. LIMIT DATA FOR SPEED ---
    print("  ⚠️ DEMO MODE: Processing only first 500 hospitals...")
    process_df = df.iloc[:500].copy()

    # --- 4. GEO & RATINGS ---
    lats, lons, ratings = [], [], []
    print("  Fetching Geo Data...")
    
    for i, r in process_df.iterrows():
        lat, lon = get_geo(r['name'], r.get('District Name', 'Kanpur'), r.get('State Name', 'Uttar Pradesh'))
        
        # Add Jitter (So dots don't overlap)
        lats.append(lat + random.uniform(-0.02, 0.02))
        lons.append(lon + random.uniform(-0.02, 0.02))
        
        # Fake Smart Rating
        base = 4.2 if r.get('is_nabh_accredited') else 3.5
        ratings.append(round(random.uniform(base, base+0.8), 1))
        
        # Progress Bar
        if i % 5 == 0: print(".", end="", flush=True)
        # time.sleep(0.5) # Removed sleep for speed since we are only doing 200

    process_df['latitude'] = lats
    process_df['longitude'] = lons
    process_df['google_rating'] = ratings
    process_df['google_ratings_total'] = [random.randint(50, 500) for _ in range(len(process_df))]
    
    # Assign Tier
    process_df['hospital_tier'] = process_df.apply(lambda x: 'A' if x['is_nabh_accredited'] else ('B' if x['google_rating']>3.8 else 'C'), axis=1)
    
    # Save Hospital CSV
    process_df.to_csv(OUT_H, index=False)
    print("\n  ✅ Hospitals saved.")

    # --- 5. CALCULATE COSTS ---
    try:
        cghs = pd.read_csv(CGHS)
        kb = pd.read_csv(KB)
        merged = kb.merge(cghs, left_on='disease_name_english', right_on='procedure_name')
        
        costs = []
        # Add IDs
        process_df['hospital_id'] = range(1, len(process_df) + 1)
        process_df.to_csv(OUT_H, index=False) 

        for _, h in process_df.iterrows():
            tier = h['hospital_tier']
            # REALISTIC PRICING LOGIC
            if tier == 'A': mult = random.uniform(5.0, 8.0) # Expensive
            elif tier == 'B': mult = random.uniform(2.5, 4.0) # Mid
            else: mult = random.uniform(1.0, 1.5) # Cheap

            for _, t in merged.iterrows():
                jitter = random.randint(-2000, 2000)
                cost = int(t['rate'] * mult) + jitter
                if cost < 500: cost = 500
                costs.append({'hospital_id':h['hospital_id'], 'treatment_id':t['treatment_id'], 'estimated_cost':cost})
                
        pd.DataFrame(costs).to_csv(OUT_C, index=False)
        print("  ✅ Costs saved.")
    except Exception as e:
        print(f"  Cost Error: {e}")

if __name__ == '__main__':
    run()
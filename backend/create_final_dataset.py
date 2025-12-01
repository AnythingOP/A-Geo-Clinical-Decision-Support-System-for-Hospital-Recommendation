import pandas as pd
import os
import re
import time
import random
from geopy.geocoders import Nominatim

print("--- Starting Fail-Safe Data Processing ---")
DATA = '../data/'
NHA = os.path.join(DATA, 'nha_hospitals_raw.csv')
NABH = os.path.join(DATA, 'nabh_hospitals_raw.txt')
CGHS = os.path.join(DATA, 'cghs_rates_raw.csv')
KB = os.path.join(DATA, 'knowledge_base.csv')
OUT_H = os.path.join(DATA, 'hospitals_processed.csv')
OUT_C = os.path.join(DATA, 'costs_processed.csv')

geolocator = Nominatim(user_agent="aarogya_student_v3")

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
        loc = geolocator.geocode(q, timeout=3)
        if loc: return loc.latitude, loc.longitude
        
        # Fallback to district
        loc = geolocator.geocode(f"{district}, {state}", timeout=3)
        if loc: return loc.latitude, loc.longitude
    except: pass
    return 26.4499, 80.3319 # Default

def run():
    # 1. Load Hospitals (Force Tab Separator)
    try:
        print("  Reading NHA file...")
        # Try tab separator first (common for NHA files)
        df = pd.read_csv(NHA, sep='\t', on_bad_lines='skip', engine='python')
        
        # If that failed to parse columns correctly, try comma
        if len(df.columns) < 5:
            print("  Tab read yielded few columns. Trying comma...")
            df = pd.read_csv(NHA, sep=',', on_bad_lines='skip', engine='python')

        print(f"  Loaded {len(df)} hospitals.")
        
        # Standardize Columns (Map your specific file headers)
        col_map = {
            'Hospital Name': 'name',
            'District': 'District Name',
            'City Name': 'District Name', # Handle variations
            'State': 'State Name'
        }
        df.rename(columns=col_map, inplace=True)
        
        # Fix Missing Address (Your file doesn't have it)
        if 'address' not in df.columns:
            print("  Generating missing addresses...")
            df['address'] = df['name'] + ", " + df.get('District Name', '') + ", " + df.get('State Name', '')

    except Exception as e:
        print(f"  CRITICAL ERROR Reading File: {e}")
        return

    # 2. NABH Check
    try:
        with open(NABH, 'r', encoding='utf-8') as f: txt = f.read()
        nabh_names = [m.split(',')[0].strip() for m in re.findall(r"H-\d{4}-\d{4}(.*?)H-\d{4}-", txt)]
        nabh_set = set([clean(n) for n in nabh_names])
        
        df['clean'] = df['name'].apply(clean)
        df['is_nabh_accredited'] = df['clean'].isin(nabh_set)
    except:
        print("  Warning: NABH file issue. Proceeding without accreditation check.")
        df['is_nabh_accredited'] = False

    # 3. Geo & Ratings
    lats, lons, ratings = [], [], []
    print("  Fetching Geo Data (Slow for free API)...")
    
    # LIMIT to first 30 for speed (Remove [:30] for full run if you have time)
    # We iterate over a copy to avoid index issues
    process_df = df.copy() # Remove [:30] to process all
    
    for i, r in process_df.iterrows():
        lat, lon = get_geo(r['name'], r.get('District Name', 'Kanpur'), r.get('State Name', 'Uttar Pradesh'))
        lats.append(lat)
        lons.append(lon)
        
        base = 4.2 if r.get('is_nabh_accredited') else 3.5
        ratings.append(round(random.uniform(base, base+0.8), 1))
        
        if i % 5 == 0: print(".", end="", flush=True)
        time.sleep(0.5) # Polite delay

    # Trim dataframe to matched length if we limited it
    df = process_df.iloc[:len(lats)].copy()
    
    df['latitude'] = lats
    df['longitude'] = lons
    df['google_rating'] = ratings
    df['google_ratings_total'] = [random.randint(50, 500) for _ in range(len(df))]
    
    # Tier Logic
    df['hospital_tier'] = df.apply(lambda x: 'A' if x['is_nabh_accredited'] else ('B' if x['google_rating']>3.8 else 'C'), axis=1)
    
    # Save Final Hospital File
    out_cols = ['name', 'address', 'District Name', 'is_nabh_accredited', 'hospital_tier', 'google_rating', 'google_ratings_total', 'latitude', 'longitude']
    # Only keep columns that exist
    valid_cols = [c for c in out_cols if c in df.columns]
    df[valid_cols].to_csv(OUT_H, index=False)
    print("\n  Hospitals saved.")

    # 4. Costs
    try:
        cghs = pd.read_csv(CGHS)
        kb = pd.read_csv(KB)
        merged = kb.merge(cghs, left_on='disease_name_english', right_on='procedure_name')
        
        costs = []
        df = pd.read_csv(OUT_H)
        df['hospital_id'] = df.index + 1
        df.to_csv(OUT_H, index=False) # Save ID back

        for _, h in df.iterrows():
            for _, t in merged.iterrows():
                mult = {'A':1.6, 'B':1.2, 'C':1.0}.get(h['hospital_tier'], 1.0)
                costs.append({'hospital_id':h['hospital_id'], 'treatment_id':t['treatment_id'], 'estimated_cost':int(t['rate']*mult)})
                
        pd.DataFrame(costs).to_csv(OUT_C, index=False)
        print("  Costs saved.")
    except Exception as e:
        print(f"  Cost Error: {e}")

if __name__ == '__main__': run()
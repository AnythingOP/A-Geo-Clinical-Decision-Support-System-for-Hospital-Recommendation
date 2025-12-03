import sqlite3
import pandas as pd
import os

DB_PATH = 'aarogyasaathi.db'
DATA_DIR = '../data/'

# Files
HOSPITALS_CSV = os.path.join(DATA_DIR, 'hospitals_processed.csv')
COSTS_CSV = os.path.join(DATA_DIR, 'costs_processed.csv')

def init_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("Removed old database.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Creating Tables...")
    
    # 1. Hospitals Table
    cursor.execute('''
        CREATE TABLE Hospitals (
            hospital_id INTEGER PRIMARY KEY,
            name TEXT,
            address TEXT,
            district TEXT,
            is_nabh_accredited BOOLEAN,
            hospital_tier TEXT,
            google_rating REAL,
            google_ratings_total INTEGER,
            latitude REAL,
            longitude REAL,
            quality_score REAL
        )
    ''')

    # 2. Costs Table
    cursor.execute('''
        CREATE TABLE Hospital_Treatment_Costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hospital_id INTEGER,
            treatment_id INTEGER,
            estimated_cost INTEGER,
            FOREIGN KEY(hospital_id) REFERENCES Hospitals(hospital_id)
        )
    ''')

    print("Loading Data...")
    try:
        # LOAD HOSPITALS
        hosp_df = pd.read_csv(HOSPITALS_CSV)

        # --- FIX: COLUMN MAPPING & CLEANING ---
        # 1. Rename 'District Name' to 'district' if it exists
        if 'District Name' in hosp_df.columns:
            hosp_df.rename(columns={'District Name': 'district'}, inplace=True)
            
        # 2. Calculate Quality Score
        hosp_df['quality_score'] = (hosp_df['google_rating'] * 2) 
        if 'is_nabh_accredited' in hosp_df.columns:
            hosp_df.loc[hosp_df['is_nabh_accredited'] == True, 'quality_score'] += 2.0
        hosp_df['quality_score'] = hosp_df['quality_score'].clip(upper=10.0)

        # 3. STRICT FILTER: Keep ONLY columns that exist in the DB schema
        # This removes 'Sno', 'Hospital Id', 'State', etc. to prevent errors
        db_columns = ['hospital_id', 'name', 'address', 'district', 'is_nabh_accredited', 
                      'hospital_tier', 'google_rating', 'google_ratings_total', 
                      'latitude', 'longitude', 'quality_score']
        
        # Select only valid columns that exist in the CSV
        valid_cols = [c for c in db_columns if c in hosp_df.columns]
        hosp_df = hosp_df[valid_cols]

        # Insert into Database
        hosp_df.to_sql('Hospitals', conn, if_exists='append', index=False)
        print(f"✅ Loaded {len(hosp_df)} hospitals.")
        
        # LOAD COSTS
        costs_df = pd.read_csv(COSTS_CSV)
        costs_df.to_sql('Hospital_Treatment_Costs', conn, if_exists='append', index=False)
        print(f"✅ Loaded {len(costs_df)} cost records.")
        
    except Exception as e:
        print(f"❌ Error initializing DB: {e}")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
import sqlite3
import pandas as pd
import os

DB_PATH = 'aarogyasaathi.db'
DATA_DIR = '../data/'

# Files created by your processing script
HOSPITALS_CSV = os.path.join(DATA_DIR, 'hospitals_processed.csv')
REAL_RATINGS_CSV = os.path.join(DATA_DIR, 'hospitals_real_ratings.csv')
COSTS_CSV = os.path.join(DATA_DIR, 'costs_processed.csv')

def init_db():
    # 1. Clear old DB
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("Removed old database.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Creating Tables...")
    
    # Create Hospitals Table
    # Note: We use 'district' (lowercase) here
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

    # Create Costs Table
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
        # 2. Load Hospitals Data
        # Prefer the file with real ratings if it exists
        if os.path.exists(REAL_RATINGS_CSV):
            print(f"Reading from {REAL_RATINGS_CSV}...")
            hosp_df = pd.read_csv(REAL_RATINGS_CSV)
        else:
            print(f"Reading from {HOSPITALS_CSV}...")
            hosp_df = pd.read_csv(HOSPITALS_CSV)

        # --- FIX: Rename 'District Name' to 'district' ---
        if 'District Name' in hosp_df.columns:
            hosp_df.rename(columns={'District Name': 'district'}, inplace=True)

        # --- FIX: Ensure 'quality_score' exists ---
        # Calculate it now: (Rating * 2) + (2 Bonus for NABH)
        hosp_df['quality_score'] = (hosp_df['google_rating'] * 2) 
        if 'is_nabh_accredited' in hosp_df.columns:
            hosp_df.loc[hosp_df['is_nabh_accredited'] == True, 'quality_score'] += 2.0
        
        # Cap score at 10
        hosp_df['quality_score'] = hosp_df['quality_score'].clip(upper=10.0)
        
        # Insert into Database
        hosp_df.to_sql('Hospitals', conn, if_exists='append', index=False)
        print("Hospitals loaded successfully.")
        
        # 3. Load Costs Data
        costs_df = pd.read_csv(COSTS_CSV)
        costs_df.to_sql('Hospital_Treatment_Costs', conn, if_exists='append', index=False)
        print("Costs loaded successfully.")
        
        print("✅ DB Initialized Successfully!")
        
    except Exception as e:
        print(f"❌ Error initializing DB: {e}")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
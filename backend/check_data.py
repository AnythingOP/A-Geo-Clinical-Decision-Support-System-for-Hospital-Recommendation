import sqlite3
import pandas as pd
import os

DB_PATH = 'aarogyasaathi.db'

if not os.path.exists(DB_PATH):
    print("❌ Database not found!")
    exit()

conn = sqlite3.connect(DB_PATH)
try:
    # Get 10 random hospitals
    df = pd.read_sql("SELECT name, latitude, longitude FROM Hospitals ORDER BY RANDOM() LIMIT 10", conn)
    print("--- HOSPITAL LOCATION CHECK ---")
    print(df)
    
    # Check if they are all identical
    first_lat = df.iloc[0]['latitude']
    is_identical = df['latitude'].eq(first_lat).all()
    
    if is_identical:
        print("\n❌ PROBLEM FOUND: All hospitals have the EXACT SAME location.")
        print("   -> The Geocoding step in 'create_final_dataset.py' failed.")
        print("   -> Solution: You must re-run 'create_final_dataset.py' with internet connected.")
    else:
        print("\n✅ DATA LOOKS GOOD: Locations are unique.")
        print("   -> The issue is likely the Routing API or Speed Settings.")

finally:
    conn.close()
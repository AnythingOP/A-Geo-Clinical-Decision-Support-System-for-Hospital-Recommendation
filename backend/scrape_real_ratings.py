import pandas as pd
import time
import random
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

DATA_DIR = '../data/'
INPUT_FILE = os.path.join(DATA_DIR, 'hospitals_processed.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'hospitals_real_ratings.csv')

def run_scraper():
    if not os.path.exists(INPUT_FILE): return print("Run create_final_dataset.py first!")
    
    df = pd.read_csv(INPUT_FILE)
    print("Starting Scraper (Window will open)...")
    
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Keep commented to see it working
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    real_ratings = []
    for i, row in df.iterrows():
        try:
            q = f"{row['name']} {row['District Name']} hospital"
            driver.get("https://www.google.com/search?q=" + q)
            time.sleep(2)
            # Try finding rating element
            try:
                rating = float(driver.find_element(By.CSS_SELECTOR, "span.Aq14fc").text)
                print(f"Found: {rating} for {row['name']}")
                real_ratings.append(rating)
            except:
                print(f"Not found for {row['name']}, keeping old.")
                real_ratings.append(row['google_rating'])
        except:
            real_ratings.append(row['google_rating'])
            
    df['google_rating'] = real_ratings
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done!")
    driver.quit()

if __name__ == '__main__': run_scraper()
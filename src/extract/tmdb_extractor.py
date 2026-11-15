import os
import json
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "77e15f0f5ed2f9e816d5c9ff3fede6cd")
RAW_DATA_PATH = os.path.join("data", "raw")

def fetch_all_40k_movies():
    today = datetime.now(timezone.utc)
    formatted_date = today.strftime("%m_%d_%Y")
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    if not TMDB_API_KEY:
        print("API Key not found!")
        return

    output_file = os.path.join(RAW_DATA_PATH, f"tmdb_movies_{formatted_date}.jsonl")
    
    max_pages = 2000  
    total_fetched = 0
    
    print("Starting to fetch 40k movies pipeline...")
    
    with open(output_file, "w", encoding="utf-8") as f:
        for page in range(1, max_pages + 1):
            discover_url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}&page={page}"
            
            try:
                response = requests.get(discover_url)
                if response.status_code == 200:
                    results = response.json().get("results", [])
                    if not results:
                        break
                        
                    for movie in results:
                        movie_id = movie["id"]
                        detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&append_to_response=credits"
                        
                        detail_res = requests.get(detail_url)
                        if detail_res.status_code == 200:
                            detailed_data = detail_res.json()
                            f.write(json.dumps(detailed_data) + "\n")
                            total_fetched += 1
                        time.sleep(0.05)
                        
                    if page % 10 == 0:
                        print(f"Processed {page} pages... Total detailed movies saved: {total_fetched}")
                else:
                    print(f"Skipped page {page} due to status code: {response.status_code}")
            except Exception as e:
                print(f"Error on page {page}: {e}")

    print(f"Done! Successfully fetched and saved {total_fetched} movies to: {output_file}")

if __name__ == "__main__":
    fetch_all_40k_movies()

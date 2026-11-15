import os
import json
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "77e15f0f5ed2f9e816d5c9ff3fede6cd")
RAW_DATA_PATH = os.path.join("data", "raw")


def fetch_all_movies():
    today = datetime.now(timezone.utc)
    formatted_date = today.strftime("%m_%d_%Y")
    
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    if not TMDB_API_KEY:
        return

    file_name = f"tmdb_movies_{formatted_date}.jsonl"
    file_path = os.path.join(RAW_DATA_PATH, file_name)
    
    total_saved = 0
    max_pages = 2000  
    
    print("Starting to fetch all movies (approx. 40k)... Please wait.")
    
    with open(file_path, "w", encoding="utf-8") as f:
        for page in range(1, max_pages + 1):
            url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}&page={page}"
            
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    movies = data.get("results", [])
                    
                    if not movies:
                        break 
                        
                    for movie in movies:
                        f.write(json.dumps(movie) + "\n")
                        total_saved += 1
                        
                    if page % 50 == 0:
                        print(f"Fetched {page} pages... Total movies saved so far: {total_saved}")
                else:
                    print(f"Skipped page {page} due to status code: {response.status_code}")
            except Exception as e:
                print(f"Error on page {page}: {e}")
                
    print(f"Done! Successfully saved a total of {total_saved} movies to: {file_path}")


if __name__ == "__main__":
    fetch_all_movies()

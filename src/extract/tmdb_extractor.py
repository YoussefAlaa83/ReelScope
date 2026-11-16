import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def process_tmdb_movies_pipeline():
    today_str = datetime.now().strftime("%m/%d/%Y")
    file_name_identifier = f"TMDB Extract - {today_str}"
    
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)
    
    date_str_for_url = datetime.now().strftime("%m_%d_%Y")
    
    movie_ids = []
    base_url = "https://api.themoviedb.org/3/discover/movie"
    
    for page in range(1, 101):
        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page
        }
        try:
            res = requests.get(base_url, params=params, timeout=10)
            if res.status_code == 200:
                results = res.json().get("results", [])
                for item in results:
                    movie_ids.append({"id": item.get("id")})
                if len(movie_ids) >= 2000:
                    movie_ids = movie_ids[:2000]
                    break
        except Exception:
            continue

    ids_file_path = os.path.join(output_dir, f"tmdb_movie_ids_{date_str_for_url}.json")
    with open(ids_file_path, "w", encoding="utf-8") as f:
        json.dump({
            "file_name": file_name_identifier,
            "total_ids": len(movie_ids),
            "items": movie_ids
        }, f, ensure_ascii=False, indent=4)

    detailed_movies = []
    headers = {"accept": "application/json"}
    
    for item in movie_ids:
        if len(detailed_movies) >= 2000:
            break
            
        movie_id = item.get("id")
        if not movie_id:
            continue
            
        detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {
            "api_key": TMDB_API_KEY,
            "append_to_response": "credits"
        }
        
        try:
            res = requests.get(detail_url, headers=headers, params=params, timeout=10)
            if res.status_code == 200:
                detailed_movies.append(res.json())
        except Exception:
            continue

    detailed_file_path = os.path.join(output_dir, f"tmdb_detailed_movies_{date_str_for_url}.json")
    detailed_output_data = {
        "file_name": file_name_identifier,
        "total_fetched": len(detailed_movies),
        "movies": detailed_movies
    }
    
    with open(detailed_file_path, "w", encoding="utf-8") as f:
        json.dump(detailed_output_data, f, ensure_ascii=False, indent=4)
        
    return detailed_output_data

if __name__ == "__main__":
    process_tmdb_movies_pipeline()

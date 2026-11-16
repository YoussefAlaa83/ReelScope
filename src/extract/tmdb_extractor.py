import os
import json
import requests
from datetime import datetime
from src.utils.config import RAW_DATA_DIR, get_tmdb_api_key

TMDB_API_KEY = get_tmdb_api_key()

def process_tmdb_movies_pipeline():
    today_str = datetime.now().strftime("%m/%d/%Y")
    file_name_identifier = f"TMDB Extract - {today_str}"
    
    output_dir = RAW_DATA_DIR
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
    seen_ids = set()
    
    for item in movie_ids:
        if len(detailed_movies) >= 2000:
            break
            
        movie_id = item.get("id")
        if not movie_id:
            continue
        if movie_id in seen_ids:
            continue
        seen_ids.add(movie_id)
            
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

def extract_tmdb():
    """Airflow entry point for the TMDb extraction pipeline."""
    return process_tmdb_movies_pipeline()

if __name__ == "__main__":
    extract_tmdb()

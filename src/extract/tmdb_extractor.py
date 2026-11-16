import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def fetch_and_save_tmdb_movies():
    base_url = "https://api.themoviedb.org/3/discover/movie"
    headers = {
        "accept": "application/json"
    }
    
    max_movies = 2000
    movies_per_page = 20
    max_pages = max_movies // movies_per_page
    
    all_movies = []
    
    for page in range(1, max_pages + 1):
        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                    
                all_movies.extend(results)
                
                if len(all_movies) >= max_movies:
                    all_movies = all_movies[:max_movies]
                    break
            else:
                break
                
        except Exception:
            break

    file_name_identifier = "TMDB Extract - 16/11/2026"
    
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, "tmdb_moviess_11_16_2026.json")
    
    output_data = {
        "file_name": file_name_identifier,
        "total_fetched": len(all_movies),
        "movies": all_movies
    }
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)
        
    return output_data

if __name__ == "__main__":
    fetch_and_save_tmdb_movies()

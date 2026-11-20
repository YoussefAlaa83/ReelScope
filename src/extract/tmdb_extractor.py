import os
import json
import glob
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from src.utils.config import RAW_DATA_DIR, REQUEST_TIMEOUT, TMDB_API_KEY, TMDB_MAX_MOVIES
from src.utils.azure_storage import list_blobs, download_blob_to_local, upload_file_to_adls

LOGGER = logging.getLogger(__name__)
CHECKPOINT_INTERVAL = 25


def _latest_checkpoint(prefix, pattern):
    """Prefer a local checkpoint; otherwise restore the latest Bronze copy."""
    local = sorted(glob.glob(os.path.join(str(RAW_DATA_DIR), pattern)), key=os.path.getmtime)
    if local:
        return local[-1]
    blobs = sorted(blob.name for blob in list_blobs(prefix) if blob.name.endswith(".json"))
    if blobs:
        local_path = os.path.join(str(RAW_DATA_DIR), os.path.basename(blobs[-1]))
        if download_blob_to_local(blobs[-1], local_path):
            return local_path
    return None


def _create_session():
    """Create a retrying HTTP session for transient TMDb failures."""
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def process_tmdb_movies_pipeline():
    today_str = datetime.now().strftime("%m/%d/%Y")
    file_name_identifier = f"TMDB Extract - {today_str}"

    output_dir = RAW_DATA_DIR
    os.makedirs(output_dir, exist_ok=True)

    date_str_for_url = datetime.now().strftime("%m_%d_%Y")
    max_movies = TMDB_MAX_MOVIES

    movie_ids = []
    base_url = "https://api.themoviedb.org/3/discover/movie"
    ids_file_path = _latest_checkpoint("raw/tmdb/tmdb_movie_ids_", "tmdb_movie_ids_*.json") or os.path.join(output_dir, f"tmdb_movie_ids_{date_str_for_url}.json")
    detailed_file_path = _latest_checkpoint("raw/tmdb/tmdb_detailed_movies_", "tmdb_detailed_movies_*.json") or os.path.join(output_dir, f"tmdb_detailed_movies_{date_str_for_url}.json")

    # Resume the current run's checkpoints and avoid requesting saved details again.
    seen_ids = set()
    if os.path.exists(ids_file_path):
        try:
            with open(ids_file_path, "r", encoding="utf-8") as file:
                saved_ids = json.load(file).get("items", [])
            for item in saved_ids:
                movie_id = item.get("id") if isinstance(item, dict) else None
                if movie_id:
                    seen_ids.add(movie_id)
                    movie_ids.append({"id": movie_id})
        except (OSError, ValueError, AttributeError):
            LOGGER.warning("Could not read TMDb ID checkpoint; starting a fresh ID queue")

    saved_detail_ids = set()
    detailed_movies = []
    if os.path.exists(detailed_file_path):
        try:
            with open(detailed_file_path, "r", encoding="utf-8") as file:
                saved_details = json.load(file).get("movies", [])
            detailed_movies = [movie for movie in saved_details if isinstance(movie, dict)]
            saved_detail_ids = {movie.get("id") for movie in detailed_movies if movie.get("id")}
        except (OSError, ValueError, AttributeError):
            LOGGER.warning("Could not read TMDb details checkpoint; continuing with ID checkpoint")

    strategies = [("vote_count.desc", 0.40), ("revenue.desc", 0.30), ("popularity.desc", 0.30)]
    quotas = [int(max_movies * share) for _, share in strategies]
    quotas[-1] += max_movies - sum(quotas)
    session = _create_session()

    for (sort_by, _), quota in zip(strategies, quotas):
        strategy_count = 0
        page = 1
        while strategy_count < quota and len(movie_ids) < max_movies and page <= 500:
            params = {
                "api_key": TMDB_API_KEY,
                "language": "en-US",
                "sort_by": sort_by,
                "vote_count.gte": 200,
                "page": page,
            }
            try:
                res = session.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
                res.raise_for_status()
                results = res.json().get("results", [])
            except (requests.RequestException, ValueError) as exc:
                LOGGER.warning("TMDb discover request failed for %s page %s: %s", sort_by, page, exc)
                break
            if not results:
                break
            for item in results:
                movie_id = item.get("id")
                if movie_id and movie_id not in seen_ids:
                    seen_ids.add(movie_id)
                    movie_ids.append({"id": movie_id})
                    strategy_count += 1
                    if strategy_count >= quota or len(movie_ids) >= max_movies:
                        break
            page += 1

    with open(ids_file_path, "w", encoding="utf-8") as f:
        json.dump({
            "file_name": file_name_identifier,
            "total_ids": len(movie_ids),
            "items": movie_ids
        }, f, ensure_ascii=False, indent=4)
    upload_file_to_adls(ids_file_path, f"raw/tmdb/{os.path.basename(ids_file_path)}")

    headers = {"accept": "application/json"}
    fetched_since_checkpoint = 0
    for item in movie_ids:
        if len(detailed_movies) >= max_movies:
            break

        movie_id = item.get("id")
        if not movie_id or movie_id in saved_detail_ids:
            continue

        detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}

        try:
            res = session.get(detail_url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            res.raise_for_status()
            detailed_movies.append(res.json())
            saved_detail_ids.add(movie_id)
            fetched_since_checkpoint += 1
            if fetched_since_checkpoint >= CHECKPOINT_INTERVAL:
                checkpoint_data = {
                    "file_name": file_name_identifier,
                    "total_fetched": len(detailed_movies),
                    "movies": detailed_movies
                }
                with open(detailed_file_path, "w", encoding="utf-8") as f:
                    json.dump(checkpoint_data, f, ensure_ascii=False, indent=4)
                upload_file_to_adls(detailed_file_path, f"raw/tmdb/{os.path.basename(detailed_file_path)}")
                fetched_since_checkpoint = 0
        except (requests.RequestException, ValueError) as exc:
            LOGGER.warning("TMDb detail request failed for movie %s: %s", movie_id, exc)

    detailed_output_data = {
        "file_name": file_name_identifier,
        "total_fetched": len(detailed_movies),
        "movies": detailed_movies
    }

    with open(detailed_file_path, "w", encoding="utf-8") as f:
        json.dump(detailed_output_data, f, ensure_ascii=False, indent=4)
    upload_file_to_adls(detailed_file_path, f"raw/tmdb/{os.path.basename(detailed_file_path)}")

    return detailed_output_data

def extract_tmdb():
    """Airflow entry point for the TMDb extraction pipeline."""
    return process_tmdb_movies_pipeline()

if __name__ == "__main__":
    extract_tmdb()

"""Extract movie metadata from OMDb, with checkpoints and API key rotation."""

import glob
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import OMDB_API_KEYS, RAW_DATA_DIR, REQUEST_TIMEOUT


LOGGER = logging.getLogger(__name__)
OMDB_URL = "https://www.omdbapi.com/"
CHECKPOINT_INTERVAL = 100
REQUEST_DELAY_SECONDS = 0.2


def _create_session() -> requests.Session:
    """Create an OMDb session with retries for transient HTTP failures."""
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _latest_tmdb_details_file() -> Path:
    """Find the most recently modified TMDb detailed movies JSON file."""
    pattern = os.path.join(str(RAW_DATA_DIR), "tmdb_detailed_movies_*.json")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No TMDb detailed movies file found in {RAW_DATA_DIR}"
        )
    return Path(max(files, key=os.path.getmtime))


def _load_imdb_ids(path: Path) -> list[str]:
    """Read unique, valid IMDb IDs while preserving their input order."""
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    movies = payload.get("movies", [])
    if not isinstance(movies, list):
        raise ValueError(f"'movies' must be a list in {path}")

    imdb_ids: list[str] = []
    seen: set[str] = set()

    for movie in movies:
        if not isinstance(movie, dict):
            continue
        imdb_id = movie.get("imdb_id")
        if isinstance(imdb_id, str):
            imdb_id = imdb_id.strip()
            if imdb_id.startswith("tt") and imdb_id not in seen:
                seen.add(imdb_id)
                imdb_ids.append(imdb_id)

    return imdb_ids


def _load_checkpoint(path: Path) -> list[dict[str, Any]]:
    """Load previously fetched OMDb records, if a checkpoint exists."""
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    movies = payload.get("movies", [])
    if not isinstance(movies, list):
        raise ValueError(f"'movies' must be a list in checkpoint {path}")

    return [movie for movie in movies if isinstance(movie, dict)]


def _not_found_path(checkpoint_path: Path) -> Path:
    """Return the sidecar path for confirmed missing IMDb IDs."""
    date_str = checkpoint_path.stem.removeprefix("omdb_movies_")
    return checkpoint_path.with_name(f"omdb_skipped_ids_{date_str}.json")


def _load_not_found_ids(path: Path) -> set[str]:
    """Load IMDb IDs confirmed absent by OMDb."""
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    ids = payload.get("not_found_ids", [])
    if not isinstance(ids, list):
        raise ValueError(f"'not_found_ids' must be a list in {path}")
    return {imdb_id for imdb_id in ids if isinstance(imdb_id, str)}


def _save_not_found_ids(path: Path, imdb_ids: set[str]) -> None:
    """Atomically persist confirmed missing IMDb IDs separately from movie records."""
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump({"not_found_ids": sorted(imdb_ids)}, file, ensure_ascii=False, indent=4)
    temporary_path.replace(path)


def _save_checkpoint(path: Path, movies: list[dict[str, Any]]) -> None:
    """Write fetched records in the standard OMDb output format."""
    output = {
        "file_name": f"OMDb Extract - {path.stem.removeprefix('omdb_movies_')}",
        "total_fetched": len(movies),
        "movies": movies,
    }
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(output, file, ensure_ascii=False, indent=4)
    temporary_path.replace(path)


def process_omdb_movies_pipeline() -> dict[str, Any]:
    """Fetch OMDb metadata for the latest TMDb movie list and checkpoint progress."""
    output_dir = Path(RAW_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_pattern = os.path.join(str(output_dir), "omdb_movies_*.json")
    checkpoint_files = [Path(path) for path in glob.glob(checkpoint_pattern)]
    output_path = (
        max(checkpoint_files, key=os.path.getmtime)
        if checkpoint_files
        else output_dir / f"omdb_movies_{datetime.now().strftime('%m_%d_%Y')}.json"
    )

    source_path = _latest_tmdb_details_file()
    imdb_ids = _load_imdb_ids(source_path)
    movies = _load_checkpoint(output_path)
    not_found_path = _not_found_path(output_path)
    not_found_ids = _load_not_found_ids(not_found_path)

    fetched_ids = {
        movie.get("imdbID")
        for movie in movies
        if isinstance(movie.get("imdbID"), str)
    }

    api_keys = list(OMDB_API_KEYS)
    if not api_keys:
        raise ValueError("OMDB_API_KEYS (or OMDB_API_KEY) is missing in .env")
    active_key_index = 0
    fetched_since_checkpoint = 0
    session = _create_session()

    LOGGER.info(
        "Starting OMDb extraction",
        extra={"event": "omdb_extraction_started", "source": "omdb"},
    )

    for imdb_id in imdb_ids:
        if imdb_id in fetched_ids or imdb_id in not_found_ids:
            continue

        key_attempts = 0
        while key_attempts < len(api_keys):
            params = {"i": imdb_id, "apikey": api_keys[active_key_index]}

            try:
                response = session.get(OMDB_URL, params=params, timeout=REQUEST_TIMEOUT)
            except requests.RequestException:
                LOGGER.exception(
                    "OMDb transient request failure; IMDb ID remains eligible for a later run",
                    extra={"event": "omdb_request_failed", "source": "omdb"},
                )
                break

            if response.status_code in (401, 429):
                LOGGER.warning(
                    "OMDb key rejected or rate limited; rotating API key",
                    extra={"event": "omdb_key_rotation", "source": "omdb"},
                )
                active_key_index = (active_key_index + 1) % len(api_keys)
                key_attempts += 1
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            try:
                response.raise_for_status()
                record = response.json()
            except (requests.RequestException, ValueError):
                LOGGER.exception(
                    "OMDb returned an invalid response; skipping IMDb ID",
                    extra={"event": "omdb_invalid_response", "source": "omdb"},
                )
                break

            if record.get("Response") == "False":
                error = str(record.get("Error", ""))
                if "Request limit reached" in error:
                    LOGGER.warning(
                        "OMDb request limit reached; rotating API key",
                        extra={"event": "omdb_key_rotation", "source": "omdb"},
                    )
                    active_key_index = (active_key_index + 1) % len(api_keys)
                    key_attempts += 1
                    time.sleep(REQUEST_DELAY_SECONDS)
                    continue

                if error == "Movie not found!":
                    not_found_ids.add(imdb_id)
                    _save_not_found_ids(not_found_path, not_found_ids)
                    LOGGER.warning(
                        "OMDb movie not found; skipping IMDb ID",
                        extra={"event": "omdb_movie_not_found", "source": "omdb"},
                    )
                else:
                    LOGGER.warning(
                        "OMDb could not fetch movie: %s",
                        error,
                        extra={"event": "omdb_movie_skipped", "source": "omdb"},
                    )
                break

            movies.append(record)
            fetched_ids.add(imdb_id)
            fetched_since_checkpoint += 1

            if fetched_since_checkpoint >= CHECKPOINT_INTERVAL:
                _save_checkpoint(output_path, movies)
                fetched_since_checkpoint = 0

            break

        else:
            _save_checkpoint(output_path, movies)
            _save_not_found_ids(not_found_path, not_found_ids)
            LOGGER.error(
                "All OMDb API keys are exhausted; progress saved. "
                "Rerun the extractor after replenishing the keys.",
                extra={"event": "omdb_keys_exhausted", "source": "omdb"},
            )
            raise RuntimeError(
                "All OMDb API keys are exhausted before extraction completed; "
                "progress was saved. Replenish the keys and rerun."
            )

        time.sleep(REQUEST_DELAY_SECONDS)

    _save_checkpoint(output_path, movies)
    _save_not_found_ids(not_found_path, not_found_ids)
    LOGGER.info(
        "OMDb extraction completed",
        extra={"event": "omdb_extraction_completed", "source": "omdb"},
    )
    return {
        "file_name": f"OMDb Extract - {output_path.stem.removeprefix('omdb_movies_')}",
        "total_fetched": len(movies),
        "movies": movies,
    }


def extract_omdb() -> dict[str, Any]:
    """Airflow entry point for the OMDb extraction pipeline."""
    return process_omdb_movies_pipeline()


if __name__ == "__main__":
    extract_omdb()

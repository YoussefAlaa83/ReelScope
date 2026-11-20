"""Extract Box Office Mojo financial and release details for TMDb movies."""

from __future__ import annotations

import glob
import json
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import RAW_DATA_DIR, REQUEST_TIMEOUT
from src.utils.azure_storage import list_blobs, download_blob_to_local, upload_file_to_adls

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://www.boxofficemojo.com"
CHECKPOINT_INTERVAL = 25
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CHALLENGE_STATUSES = {202, 403, 429}


def _create_session() -> requests.Session:
    """Create a browser-like session with retries for transient server errors."""
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _latest_tmdb_details_file() -> Path:
    """Return the newest detailed TMDb input file in the configured raw folder."""
    paths = [Path(p) for p in glob.glob(str(Path(RAW_DATA_DIR) / "tmdb_detailed_movies_*.json"))]
    if paths:
        return max(paths, key=lambda path: path.stat().st_mtime)
    blobs = sorted(blob.name for blob in list_blobs("raw/tmdb/tmdb_detailed_movies_") if blob.name.endswith(".json"))
    if blobs:
        restored = Path(RAW_DATA_DIR) / Path(blobs[-1]).name
        if download_blob_to_local(blobs[-1], restored):
            return restored
    raise FileNotFoundError(f"No TMDb detailed movies file found locally or in Azure Bronze ({RAW_DATA_DIR})")


def _load_imdb_ids(path: Path) -> list[str]:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    movies = payload.get("movies", [])
    if not isinstance(movies, list):
        raise ValueError(f"'movies' must be a list in {path}")
    ids: list[str] = []
    seen: set[str] = set()
    for movie in movies:
        imdb_id = movie.get("imdb_id") if isinstance(movie, dict) else None
        if isinstance(imdb_id, str) and re.fullmatch(r"tt\d+", imdb_id.strip()):
            imdb_id = imdb_id.strip()
            if imdb_id not in seen:
                seen.add(imdb_id)
                ids.append(imdb_id)
    return ids


def _output_paths() -> tuple[Path, Path]:
    """Reuse newest output pair irrespective of today's date, for midnight-safe resume."""
    folder = Path(RAW_DATA_DIR)
    files = [Path(p) for p in glob.glob(str(folder / "bom_movies_*.json"))]
    if files:
        output = max(files, key=lambda path: path.stat().st_mtime)
    else:
        blobs = list_blobs("raw/bom/bom_movies_")
        matching = sorted((blob.name for blob in blobs if blob.name.endswith(".json")), reverse=True)
        if matching:
            blob_name = matching[0]
            output = folder / Path(blob_name).name
            download_blob_to_local(blob_name, output)
        else:
            output = folder / f"bom_movies_{datetime.now():%m_%d_%Y}.json"
    date_suffix = output.stem.removeprefix("bom_movies_")
    skipped = folder / f"bom_skipped_ids_{date_suffix}.json"
    if not skipped.exists():
        for blob in list_blobs(f"raw/bom/bom_skipped_ids_{date_suffix}.json"):
            download_blob_to_local(blob.name, skipped)
            break
    return output, skipped


def _read_json(path: Path, key: str) -> list[Any]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    value = payload.get(key, [])
    if not isinstance(value, list):
        raise ValueError(f"'{key}' must be a list in {path}")
    return value


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    temp.replace(path)


def _save(output_path: Path, skipped_path: Path, movies: list[dict[str, Any]], skipped: set[str]) -> None:
    _atomic_json(output_path, {
        "file_name": f"Box Office Mojo Extract - {output_path.stem.removeprefix('bom_movies_')}",
        "total_fetched": len(movies),
        "movies": movies,
    })
    _atomic_json(skipped_path, {"not_found_ids": sorted(skipped)})


def _text(element: Any) -> str | None:
    if not element:
        return None
    value = " ".join(element.stripped_strings)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s*(?:See full company information|See full company profile).*?$", "", value, flags=re.I).strip(" |·")
    return value or None


def _money(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\$\s*([\d,]+)", value)
    return int(match.group(1).replace(",", "")) if match else None


def _label_value(soup: BeautifulSoup, labels: tuple[str, ...]) -> str | None:
    wanted = {label.casefold() for label in labels}
    for element in soup.select("span, div, dt, th"):
        label = _text(element)
        if not label or label.rstrip(":").casefold() not in wanted:
            continue
        for candidate in (element.find_next_sibling(), element.parent.find_next_sibling() if element.parent else None,
                          element.find_next(["span", "div", "a", "td", "dd"])):
            value = _text(candidate)
            if value and value.casefold() not in wanted:
                return value
    return None


def _release_link(soup: BeautifulSoup) -> str | None:
    # Prefer the first release listed; BOM places the primary/original release first.
    for anchor in soup.select('a[href*="/release/rl"]'):
        href = anchor.get("href", "")
        if re.search(r"/release/rl\d+/?", href):
            return urljoin(BASE_URL, href)
    return None


def _parse_page(html: str, imdb_id: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    title = _text(h1)
    if not title or "captcha" in html[:10000].casefold() or "verify you are human" in html[:10000].casefold():
        return None
    amounts: dict[str, int | None] = {"domestic_revenue": None, "international_revenue": None,
                                     "worldwide_revenue": None, "opening_weekend": None}
    for row in soup.select("tr"):
        cells = [_text(cell) for cell in row.find_all(["td", "th"], recursive=False)]
        if len(cells) < 2:
            continue
        label = (cells[0] or "").casefold()
        amount = _money(" ".join(value or "" for value in cells[1:]))
        if "domestic" in label and "opening" not in label:
            amounts["domestic_revenue"] = amount
        elif "international" in label:
            amounts["international_revenue"] = amount
        elif "worldwide" in label:
            amounts["worldwide_revenue"] = amount
        elif "opening" in label:
            amounts["opening_weekend"] = amount
    for key, labels in {
        "domestic_revenue": ("Domestic", "Domestic Box Office"),
        "international_revenue": ("International", "International Box Office"),
        "worldwide_revenue": ("Worldwide", "Worldwide Box Office"),
        "opening_weekend": ("Opening", "Opening Weekend", "Opening Weekend Box Office"),
    }.items():
        amounts[key] = amounts[key] or _money(_label_value(soup, labels))
    distributor = _label_value(soup, ("Distributor",))
    release_date = _label_value(soup, ("Release Date", "Release"))
    rating = _label_value(soup, ("MPAA", "MPAA Rating"))
    return {
        "imdb_id": imdb_id, "title": title, **amounts,
        "distributor": distributor, "mpaa_rating": rating,
        "release_date_bom": release_date,
    }


def _fetch(session: requests.Session, url: str, imdb_id: str) -> tuple[int, str]:
    time.sleep(random.uniform(1.2, 2.5))
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    return response.status_code, response.text


def _scrape_movie(session: requests.Session, imdb_id: str) -> tuple[str, dict[str, Any] | None]:
    url = f"{BASE_URL}/title/{imdb_id}/"
    try:
        status, html = _fetch(session, url, imdb_id)
    except requests.RequestException:
        LOGGER.exception("BOM request failed; ID remains eligible for a later run", extra={"event": "bom_request_failed", "source": "bom"})
        return "retry", None
    if status == 404:
        return "missing", None
    if status in CHALLENGE_STATUSES or not html.strip():
        LOGGER.warning("BOM returned a challenge or empty response; backing off without skipping ID", extra={"event": "bom_challenge", "source": "bom"})
        time.sleep(10)
        return "retry", None
    if status >= 400:
        LOGGER.warning("BOM returned HTTP %s; ID remains eligible", status, extra={"event": "bom_http_error", "source": "bom"})
        return "retry", None
    record = _parse_page(html, imdb_id)
    if record is None:
        LOGGER.warning("BOM page has no valid title heading; backing off without skipping ID", extra={"event": "bom_invalid_page", "source": "bom"})
        time.sleep(10)
        return "retry", None
    if record["opening_weekend"] is None or record["distributor"] is None:
        detail_url = _release_link(BeautifulSoup(html, "lxml"))
        if detail_url:
            try:
                detail_status, detail_html = _fetch(session, detail_url, imdb_id)
                if detail_status in CHALLENGE_STATUSES or not detail_html.strip():
                    LOGGER.warning("BOM release detail returned a challenge; backing off", extra={"event": "bom_release_challenge", "source": "bom"})
                    time.sleep(10)
                    return "retry", None
                if detail_status == 200:
                    detail = _parse_page(detail_html, imdb_id)
                    if detail:
                        for key in ("opening_weekend", "distributor", "release_date_bom", "mpaa_rating"):
                            record[key] = record[key] or detail[key]
            except requests.RequestException:
                LOGGER.exception("BOM release detail request failed", extra={"event": "bom_release_request_failed", "source": "bom"})
    return "ok", record


def _upload_to_azure(paths: tuple[Path, Path]) -> None:
    """Best-effort upload of BOM raw files through the shared Azure helper."""
    for path in paths:
        upload_file_to_adls(path, f"raw/bom/{path.name}")


def extract_bom(limit: int | None = None) -> dict[str, Any]:
    """Scrape BOM details for TMDb movies, checkpointing every 25 successes."""
    source_path = _latest_tmdb_details_file()
    imdb_ids = _load_imdb_ids(source_path)
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        imdb_ids = imdb_ids[:limit]
    output_path, skipped_path = _output_paths()
    movies = [row for row in _read_json(output_path, "movies") if isinstance(row, dict)]
    skipped = {value for value in _read_json(skipped_path, "not_found_ids") if isinstance(value, str)}
    completed = {row.get("imdb_id") for row in movies if isinstance(row.get("imdb_id"), str)}
    session = _create_session()
    since_checkpoint = 0
    LOGGER.info("Starting BOM extraction from %s", source_path.name, extra={"event": "bom_extraction_started", "source": "bom"})
    for imdb_id in imdb_ids:
        if imdb_id in completed or imdb_id in skipped:
            continue
        try:
            outcome, record = _scrape_movie(session, imdb_id)
        except requests.RequestException:
            LOGGER.exception("BOM request failed; ID remains eligible for a later run", extra={"event": "bom_request_failed", "source": "bom"})
            continue
        if outcome == "missing":
            skipped.add(imdb_id)
            _save(output_path, skipped_path, movies, skipped)
            continue
        if outcome == "ok" and record is not None:
            movies.append(record)
            completed.add(imdb_id)
            since_checkpoint += 1
            if since_checkpoint >= CHECKPOINT_INTERVAL:
                _save(output_path, skipped_path, movies, skipped)
                _upload_to_azure((output_path, skipped_path))
                since_checkpoint = 0
    _save(output_path, skipped_path, movies, skipped)
    _upload_to_azure((output_path, skipped_path))
    LOGGER.info("BOM extraction completed", extra={"event": "bom_extraction_completed", "source": "bom"})
    return {"file_name": f"Box Office Mojo Extract - {output_path.stem.removeprefix('bom_movies_')}",
            "total_fetched": len(movies), "movies": movies}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of input movies to process")
    args = parser.parse_args()
    extract_bom(limit=args.limit)

"""Central configuration, environment loading, and logging for ReelScope.

Secrets are read from the project ``.env`` file (or the process environment).
Call :func:`load_settings` at application startup to validate required values.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line for Airflow/container logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key in ("event", "task_id", "source"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str | None = None) -> None:
    """Configure root logging with JSON output; safe to call more than once."""
    configured_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, configured_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid LOG_LEVEL {configured_level!r}; use DEBUG, INFO, WARNING, ERROR, or CRITICAL")
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)


@dataclass(frozen=True)
class Settings:
    """Validated runtime settings for extractors and the SQL Server loader."""

    tmdb_api_key: str
    omdb_api_keys: tuple[str, ...]
    db_server: str
    db_name: str
    db_driver: str
    db_user: str | None
    db_password: str | None
    db_trusted_connection: bool
    project_root: Path = PROJECT_ROOT
    data_dir: Path = DATA_DIR
    raw_data_dir: Path = RAW_DATA_DIR


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean (true/false, 1/0, yes/no, on/off)")


def load_settings() -> Settings:
    """Load and validate API and SQL Server settings from environment variables.

    SQL authentication is selected when both ``DB_USER`` and ``DB_PASSWORD``
    are supplied. Otherwise set ``DB_TRUSTED_CONNECTION=true`` for Windows auth.
    """
    tmdb_api_key = _required("TMDB_API_KEY")
    if tmdb_api_key.lower().startswith(("your_", "replace_with_")):
        raise ValueError("TMDB_API_KEY still contains an example placeholder")

    omdb_api_keys = tuple(key.strip() for key in _required("OMDB_API_KEYS").split(",") if key.strip())
    if not omdb_api_keys:
        raise ValueError("OMDB_API_KEYS must contain at least one comma-separated API key")
    if any(key.lower().startswith(("your_", "replace_with_")) for key in omdb_api_keys):
        raise ValueError("OMDB_API_KEYS contains an example placeholder")

    db_user = os.getenv("DB_USER", "").strip() or None
    db_password = os.getenv("DB_PASSWORD", "") or None
    trusted = _boolean("DB_TRUSTED_CONNECTION", default=not (db_user and db_password))
    if bool(db_user) != bool(db_password):
        raise ValueError("Set both DB_USER and DB_PASSWORD, or neither")
    if trusted and db_user:
        raise ValueError("Choose either DB_TRUSTED_CONNECTION or DB_USER/DB_PASSWORD, not both")
    if not trusted and not db_user:
        raise ValueError("SQL authentication requires DB_USER and DB_PASSWORD")

    return Settings(
        tmdb_api_key=tmdb_api_key,
        omdb_api_keys=omdb_api_keys,
        db_server=_required("DB_SERVER"),
        db_name=_required("DB_NAME"),
        db_driver=_required("DB_DRIVER"),
        db_user=db_user,
        db_password=db_password,
        db_trusted_connection=trusted,
    )


configure_logging()

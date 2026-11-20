"""Optional Azure Blob Storage helpers for the ADLS Gen2 Bronze layer.

When Azure credentials are absent, operations are safely skipped so local
``data/raw`` files remain the authoritative cache for development runs.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.utils.config import AZURE_CONTAINER_BRONZE, AZURE_STORAGE_CONNECTION_STRING

LOGGER = logging.getLogger(__name__)


def _container_name(container: str | None) -> str:
    return (container or AZURE_CONTAINER_BRONZE).strip() or AZURE_CONTAINER_BRONZE


def _service_client() -> Any | None:
    if not AZURE_STORAGE_CONNECTION_STRING:
        LOGGER.info("Azure storage is not configured; using local files only", extra={"event": "azure_storage_disabled"})
        return None
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        LOGGER.warning("Azure storage is configured but azure-storage-blob is unavailable", extra={"event": "azure_sdk_missing"})
        return None
    try:
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except Exception:
        LOGGER.exception("Could not initialize Azure Blob Storage client", extra={"event": "azure_client_failed"})
        return None


def upload_file_to_adls(local_path: str | Path, blob_name: str | None = None,
                        container: str | None = None) -> bool:
    """Upload a local file; return False for disabled storage or upload errors."""
    path = Path(local_path)
    if not path.is_file():
        LOGGER.warning("Azure upload source does not exist: %s", path, extra={"event": "azure_upload_source_missing"})
        return False
    service = _service_client()
    if service is None:
        return False
    target = blob_name or f"raw/{path.name}"
    try:
        client = service.get_blob_client(container=_container_name(container), blob=target)
        with path.open("rb") as file:
            client.upload_blob(file, overwrite=True)
        LOGGER.info("Uploaded %s to Azure container %s", target, _container_name(container), extra={"event": "azure_upload_completed"})
        return True
    except Exception:
        LOGGER.exception("Azure upload failed for %s", target, extra={"event": "azure_upload_failed"})
        return False


def download_blob_to_local(blob_name: str, local_path: str | Path,
                           container: str | None = None) -> bool:
    """Download a blob to a local path; return False if unavailable or failed."""
    service = _service_client()
    if service is None:
        return False
    path = Path(local_path)
    try:
        client = service.get_blob_client(container=_container_name(container), blob=blob_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = client.download_blob().readall()
        path.write_bytes(payload)
        LOGGER.info("Downloaded Azure blob %s", blob_name, extra={"event": "azure_download_completed"})
        return True
    except Exception:
        LOGGER.exception("Azure download failed for %s", blob_name, extra={"event": "azure_download_failed"})
        return False


def blob_exists(blob_name: str, container: str | None = None) -> bool:
    """Return whether a blob exists; return False when Azure is unavailable."""
    service = _service_client()
    if service is None:
        return False
    try:
        return bool(service.get_blob_client(container=_container_name(container), blob=blob_name).exists())
    except Exception:
        LOGGER.exception("Could not check Azure blob %s", blob_name, extra={"event": "azure_exists_failed"})
        return False


def list_blobs(prefix: str = "", container: str | None = None) -> list[Any]:
    """List blobs in a container, optionally filtered by name prefix."""
    service = _service_client()
    if service is None:
        return []
    try:
        return list(service.get_container_client(_container_name(container)).list_blobs(name_starts_with=prefix or None))
    except Exception:
        LOGGER.exception("Could not list Azure blobs with prefix %s", prefix, extra={"event": "azure_list_failed"})
        return []

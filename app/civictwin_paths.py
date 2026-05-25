"""Path helpers for read-only CivicTwin assets used by the app."""

import os
from pathlib import Path

from flask import current_app


def _app_or_env(name: str, default: str) -> str:
    try:
        value = current_app.config.get(name)
    except RuntimeError:
        value = None
    if value:
        return str(value)
    return os.environ.get(name, default)


def civictwin_root() -> Path:
    return Path(_app_or_env("CIVICTWIN_ROOT", "/Volumes/DigitalTwin/CivicTwin"))


def parcel_geojson_path() -> Path:
    custom = _app_or_env("PARCEL_GEOJSON", "")
    if custom:
        return Path(custom)
    return civictwin_root() / "gis" / "dennis_parcels.geojson"


def registry_scan_path(book: str, page: str) -> Path:
    safe_book = str(book).strip().replace("/", "_")
    safe_page = str(page).strip().replace("/", "_")
    return civictwin_root() / "registry" / "documents" / safe_book / safe_page / "scan.pdf"


def town_doc_pdf_path(source_path: str) -> Path:
    return civictwin_root() / "ma-dennis" / source_path

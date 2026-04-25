"""Shared cache management for the Barnstable Registry pipeline."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from discovery.config import get_config

STALENESS_DAYS = 30

log = logging.getLogger(__name__)


def _registry_dir() -> Path:
    return get_config().output_dir("registry")


def ensure_cache_dirs() -> None:
    root = _registry_dir()
    for d in [
        root / "index",
        root / "documents",
        root / "landcourt",
        root / "queue",
    ]:
        d.mkdir(parents=True, exist_ok=True)
    log.debug("Registry cache directories verified at %s", root)


# ── Index cache (enumerate pass) ─────────────────────────────────────────────

def _index_dir(parcel_id: str) -> Path:
    safe = parcel_id.replace("/", "_").replace("\\", "_")
    return _registry_dir() / "index" / safe


def index_path(parcel_id: str) -> Path:
    return _index_dir(parcel_id) / "documents.json"


def last_checked_path(parcel_id: str) -> Path:
    return _index_dir(parcel_id) / "last_checked.txt"


def is_index_fresh(parcel_id: str, threshold_days: int = STALENESS_DAYS) -> bool:
    lc = last_checked_path(parcel_id)
    if not lc.exists():
        return False
    try:
        ts = datetime.fromisoformat(lc.read_text().strip())
        age = (datetime.now(timezone.utc) - ts).days
        return age < threshold_days
    except Exception:
        return False


def get_cached_index(parcel_id: str) -> list | None:
    p = index_path(parcel_id)
    if not p.exists():
        return None
    if not is_index_fresh(parcel_id):
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.warning("Failed to read cached index for %s: %s", parcel_id, e)
        return None


def save_index(parcel_id: str, docs: list) -> None:
    d = _index_dir(parcel_id)
    d.mkdir(parents=True, exist_ok=True)
    index_path(parcel_id).write_text(json.dumps(docs, indent=2))
    last_checked_path(parcel_id).write_text(datetime.now(timezone.utc).isoformat())
    log.debug("Saved index for %s: %d documents", parcel_id, len(docs))


# ── Document scan cache (download pass) ──────────────────────────────────────

def _doc_dir(book: str, page: str) -> Path:
    safe_book = str(book).strip().replace("/", "_")
    safe_page = str(page).strip().replace("/", "_")
    return _registry_dir() / "documents" / safe_book / safe_page


def scan_path(book: str, page: str) -> Path:
    return _doc_dir(book, page) / "scan.pdf"


def metadata_path(book: str, page: str) -> Path:
    return _doc_dir(book, page) / "metadata.json"


def scan_exists(book: str, page: str) -> bool:
    return scan_path(book, page).exists()


# ── Land Court cache ──────────────────────────────────────────────────────────

def _lc_dir(certificate: str) -> Path:
    safe = str(certificate).strip().replace("/", "_")
    return _registry_dir() / "landcourt" / safe


def lc_scan_path(certificate: str) -> Path:
    return _lc_dir(certificate) / "scan.pdf"


def lc_metadata_path(certificate: str) -> Path:
    return _lc_dir(certificate) / "metadata.json"


def lc_scan_exists(certificate: str) -> bool:
    return lc_scan_path(certificate).exists()


# ── Utilities ─────────────────────────────────────────────────────────────────

def all_cached_indexes() -> list[tuple[str, list]]:
    """Return [(parcel_id, docs), ...] for all cached index files."""
    results = []
    index_root = _registry_dir() / "index"
    if not index_root.exists():
        return results
    for p in sorted(index_root.glob("*/documents.json")):
        parcel_id = p.parent.name.replace("_", "-")
        try:
            docs = json.loads(p.read_text())
            results.append((parcel_id, docs))
        except Exception as e:
            log.warning("Could not read %s: %s", p, e)
    return results

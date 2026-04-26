"""Shared cache management for the Barnstable Registry pipeline."""

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from discovery.config import LOCAL_OUTPUT_DIR, get_config

# ── Staleness windows by entry type ──────────────────────────────────────────
#
# Parcel entries (Tier 1/2): refreshed yearly. New recordings are caught by
# the sweep; parcel deed records themselves rarely change.
#
# Sweep entries (sweep-denn-*): refreshed monthly. The Town of Dennis records
# new instruments continuously; we want to catch them within a month.
#
# Xref entries (xref-*): refreshed quarterly.

_STALENESS: dict[str, int] = {
    "sweep-denn-":  30,
    "xref-":        90,
}
_PARCEL_STALENESS = 365

# Jitter cap as a fraction of the staleness window. Entries expire between
# (1 - JITTER_FRACTION) * staleness and staleness days from write time,
# spreading load uniformly across that range.
_JITTER_FRACTION = 0.75


def _staleness_for(parcel_id: str) -> int:
    for prefix, days in _STALENESS.items():
        if parcel_id.startswith(prefix):
            return days
    return _PARCEL_STALENESS


def _spread_jitter(parcel_id: str, staleness_days: int) -> int:
    """Deterministic jitter in [0, staleness * JITTER_FRACTION) from parcel_id.

    Applied on write so that entries expire spread across the staleness window
    rather than all at once. The same parcel_id always gets the same jitter,
    so re-fetching an entry resets it to the same slot in the expiry schedule.
    """
    max_jitter = int(staleness_days * _JITTER_FRACTION)
    h = int(hashlib.md5(parcel_id.encode()).hexdigest(), 16)
    return h % max(max_jitter, 1)


_LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
_LOG_DATEFMT = "%H:%M:%S"


def setup_logging(label: str) -> Path:
    """Add a timestamped file handler to the root logger. Returns the log path."""
    log_dir = LOCAL_OUTPUT_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{label}_{ts}.log"
    handler = logging.FileHandler(log_path)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    logging.getLogger().addHandler(handler)
    return log_path


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


def is_index_fresh(parcel_id: str, threshold_days: int | None = None) -> bool:
    lc = last_checked_path(parcel_id)
    if not lc.exists():
        return False
    if threshold_days is None:
        threshold_days = _staleness_for(parcel_id)
    try:
        ts = datetime.fromisoformat(lc.read_text().strip())
        age = (datetime.now(timezone.utc) - ts).days
        return age < threshold_days
    except Exception:
        return False


def _truncated_flag_path(parcel_id: str) -> Path:
    return _index_dir(parcel_id) / "truncated.flag"


def get_cached_index(parcel_id: str) -> list | None:
    p = index_path(parcel_id)
    if not p.exists():
        return None
    if not is_index_fresh(parcel_id):
        return None
    if _truncated_flag_path(parcel_id).exists():
        log.debug("Cache entry %s is truncated — treating as stale", parcel_id)
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.warning("Failed to read cached index for %s: %s", parcel_id, e)
        return None


def save_index(parcel_id: str, docs: list, truncated: bool = False) -> None:
    d = _index_dir(parcel_id)
    d.mkdir(parents=True, exist_ok=True)
    index_path(parcel_id).write_text(json.dumps(docs, indent=2))
    staleness = _staleness_for(parcel_id)
    jitter = _spread_jitter(parcel_id, staleness)
    checked_at = datetime.now(timezone.utc) - timedelta(days=jitter)
    last_checked_path(parcel_id).write_text(checked_at.isoformat())
    flag = _truncated_flag_path(parcel_id)
    if truncated:
        flag.touch()
    elif flag.exists():
        flag.unlink()
    log.debug("Saved index for %s: %d documents%s",
              parcel_id, len(docs), " [TRUNCATED]" if truncated else "")


def spread_expiry() -> int:
    """Retroactively spread last_checked timestamps across each entry's staleness window.

    Run once after the initial load to prevent all entries from expiring
    simultaneously. Safe to re-run; idempotent (same parcel always gets the
    same jitter offset). Returns the number of entries updated.
    """
    index_root = _registry_dir() / "index"
    if not index_root.exists():
        return 0
    count = 0
    now = datetime.now(timezone.utc)
    for lc_path in sorted(index_root.glob("*/last_checked.txt")):
        parcel_id = lc_path.parent.name.replace("_", "-")
        staleness = _staleness_for(parcel_id)
        jitter = _spread_jitter(parcel_id, staleness)
        lc_path.write_text((now - timedelta(days=jitter)).isoformat())
        count += 1
    return count


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

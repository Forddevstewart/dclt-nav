"""
Stage 2: Download PDFs for Documents with status='pending'.
"""

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from discovery.config import get_config
from discovery.agenda_center.db import get_session
from discovery.agenda_center.models import DocStatus, Document

log = logging.getLogger(__name__)


def _assets_dir() -> Path:
    return get_config().output_dir("agenda_center")


def _slugify(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name or "unknown"


def _filename(url: str, doc_type: str) -> str:
    segment = url.rstrip("/").rsplit("/", 1)[-1].lstrip("_")
    return f"{doc_type}_{segment}.pdf"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_one(doc: Document) -> tuple[bool, bool]:
    """Download one document. Returns (success, fetched_from_network)."""
    assets_dir = _assets_dir()
    dest_dir = assets_dir / _slugify(doc.committee_name or "unknown")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / _filename(doc.url, doc.doc_type.value)

    if dest.exists():
        with open(dest, "rb") as f:
            magic = f.read(4)
        if magic == b"%PDF":
            doc.local_path = str(dest)
            doc.checksum = _sha256(dest)
            doc.file_size_bytes = dest.stat().st_size
            doc.status = DocStatus.downloaded
            log.debug("  SKIP (exists)  %s / %s", doc.committee_name, dest.name)
            return True, False
        else:
            dest.unlink()

    try:
        resp = requests.get(doc.url, timeout=30, stream=True)
        resp.raise_for_status()
        content = resp.content
        if not content.startswith(b"%PDF"):
            raise ValueError(f"Response is not a PDF (got {content[:20]!r})")
        with open(dest, "wb") as f:
            f.write(content)
        doc.local_path = str(dest)
        doc.checksum = _sha256(dest)
        doc.file_size_bytes = dest.stat().st_size
        doc.downloaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
        doc.status = DocStatus.downloaded
        log.info("  OK  %s / %s (%s bytes)", doc.committee_name, dest.name, f"{doc.file_size_bytes:,}")
        return True, True
    except Exception as exc:
        doc.status = DocStatus.error
        log.error("  ERR %s: %s", doc.url, exc)
        if dest.exists():
            dest.unlink()
        return False, True


def run(limit: int | None = None, delay: float = 1.0) -> tuple[int, int]:
    """Download pending documents. Returns (ok, errors)."""
    ok = errors = 0
    with get_session() as session:
        q = session.query(Document).filter(Document.status == DocStatus.pending)
        if limit:
            q = q.limit(limit)
        pending = q.all()

        if not pending:
            log.info("No pending documents to download")
            return 0, 0

        assets_dir = _assets_dir()
        log.info("Downloading %d document(s) → %s (delay=%.1fs)", len(pending), assets_dir, delay)
        last_fetched = False
        try:
            for doc in pending:
                if last_fetched and delay > 0:
                    time.sleep(delay)
                result, fetched = _download_one(doc)
                last_fetched = fetched
                if result:
                    ok += 1
                else:
                    errors += 1
        except KeyboardInterrupt:
            log.info("Interrupted — committing progress (%d ok, %d errors so far)", ok, errors)

    log.info("Downloaded: %d  Errors: %d", ok, errors)
    return ok, errors

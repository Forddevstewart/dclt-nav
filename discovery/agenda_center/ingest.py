"""
Stage 1: Load civic-scraper CSV metadata into the Document table.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

from discovery.agenda_center.db import init_db, get_session
from discovery.agenda_center.models import DocStatus, DocType, Document
from discovery.config import LOCAL_OUTPUT_DIR

METADATA_DIR = LOCAL_OUTPUT_DIR / "agendacenter_metadata"

log = logging.getLogger(__name__)

ASSET_TYPE_MAP = {
    "agenda":        DocType.agenda,
    "minutes":       DocType.minutes,
    "agenda_packet": DocType.packet,
    "supplemental":  DocType.supplemental,
}


def latest_csv() -> Path | None:
    matches = sorted(METADATA_DIR.glob("civic_scraper_assets_meta_*.csv"))
    return matches[-1] if matches else None


def _parse_date(value: str):
    if not value or not value.strip():
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def run(csv_path: Path | None = None) -> int:
    """Ingest a metadata CSV. Returns count of new documents added."""
    init_db()

    if csv_path is None:
        csv_path = latest_csv()
    if csv_path is None:
        log.warning("No metadata CSV found in %s — run scrape first", METADATA_DIR)
        return 0

    log.info("Ingesting %s", csv_path)

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        log.info("CSV is empty")
        return 0

    new_count = skipped = 0

    with get_session() as session:
        existing_urls = {url for (url,) in session.query(Document.url).all()}

        for row in rows:
            url = row.get("url", "").strip()
            if not url or url in existing_urls:
                skipped += 1
                continue

            asset_type = row.get("asset_type", "").strip().lower()
            size_raw = row.get("content_length", "").strip()

            doc = Document(
                url=url,
                doc_type=ASSET_TYPE_MAP.get(asset_type, DocType.unknown),
                committee_name=row.get("committee_name", "").strip() or None,
                meeting_date=_parse_date(row.get("meeting_date", "")),
                file_size_bytes=int(size_raw) if size_raw.isdigit() else None,
                status=DocStatus.pending,
            )
            session.add(doc)
            existing_urls.add(url)
            new_count += 1

    log.info("Ingested: %d new, %d skipped (already in DB)", new_count, skipped)
    return new_count

"""Registry download pass — selective download of document scans.

Reads all cached documents.json manifests, builds a download queue filtered
to approved instrument types, reports it, and (after confirmation) downloads
scan PDFs to the local cache.

Run separately after enumerate pass completes. Do NOT run automatically.

Usage:
    python3 -m discovery.registry.download [--override-robots] [--confirm]
                                           [--limit N]
"""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from discovery.config import LOCAL_OUTPUT_DIR
from discovery.registry.cache import (
    ensure_cache_dirs,
    all_cached_indexes,
    scan_path, metadata_path, scan_exists,
    setup_logging,
)
from discovery.config import get_config as _get_config
from discovery.registry.ratelimit import RateLimiter, RegistryThrottleError, check_robots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

REGISTRY_BASE = "https://search.barnstabledeeds.org"
REPORT_TXT = LOCAL_OUTPUT_DIR / "registry_download_report.txt"

APPROVED_INSTRUMENT_TYPES = {
    "CX", "TK", "VO", "MD", "RS", "CV", "AR", "FD", "DD/LC", "DL/TR", "AG",
    "CONSERVATION RESTRICTION", "TAKING", "ORDER OF TAKING",
    "CERTIFICATE OF VOTE", "VOTE", "MASTER DEED",
    "RESTRICTION", "COVENANT", "VOLUNTARY RESTRICTION",
    "DECLARATION OF COVENANTS", "SPECIAL PERMIT", "APPROVAL",
    "ACCEPTANCE OF DEED", "ACCEPTANCE", "DECLARATION OF TRUST",
    "AGREEMENT OR INDENTURE", "FINAL DECREE",
    "DEED", "TIME SHARING DEED", "FORECLOSURE DEED", "QUITCLAIM DEED",
    "EASEMENT", "COURT ORDER", "NOTICE", "ABSTRACT OF TRUST",
    "AFFIDAVIT", "ORDER", "AMENDMENT", "LEASE", "LICENSE",
}

SKIP_KEYWORDS = {
    "MORTGAGE", "DISCHARGE", "ASSIGNMENT", "ATTACHMENT", "LIS PENDENS",
    "RELEASE", "SUBORDINATION", "BETTERMENT", "TAX TAKING",
    "FINANCE STATEMENT", "TERMINATION", "PARTIAL DISCHARGE",
    "MUNICIPAL LIEN", "DEATH CERTIFICATE", "MARRIAGE CERTIFICATE",
    "DECLARATION OF HOMESTEAD", "DISCHARGE OF HOMESTEAD",
    "ESTATE TAX", "REVENUE LIEN", "LIEN",
}

SKIP_TYPE_CODES = {
    "M", "D/AF", "DMAFS", "D/H", "AT", "LP",
    "AS", "AS/LS", "AS/DD", "AS/BS", "AS/FS", "LS/AS",
    "PD", "PD/FS", "PD/LX", "SD", "SD/FS", "SD/LX",
    "BC", "DC", "MC", "FS", "CR/FS", "TM/FS", "L/X",
    "TX/TK", "RD", "R/ET", "R/LX", "R/MH", "R/CX", "DD/SX",
}


def _is_approved(doc: dict) -> bool:
    code = (doc.get("doc_type_code") or "").upper().strip()
    itype = (doc.get("instrument_type") or "").upper().strip()
    if code in SKIP_TYPE_CODES:
        return False
    for kw in SKIP_KEYWORDS:
        if kw in itype or kw in code:
            return False
    if code in APPROVED_INSTRUMENT_TYPES:
        return True
    for approved in APPROVED_INSTRUMENT_TYPES:
        if len(approved) > 3 and approved in itype:
            return True
    return False


def _doc_key(doc: dict) -> str:
    book = (doc.get("book") or "").strip()
    page = (doc.get("page") or "").strip()
    if book and page:
        return f"{book}-{page}"
    return (doc.get("image_id") or "").strip()


def build_download_manifest() -> tuple[list[dict], dict]:
    all_data = all_cached_indexes()
    if not all_data:
        return [], {}

    doc_map: dict[str, dict] = {}
    parcel_refs: dict[str, list[str]] = {}

    for pid, docs in all_data:
        for doc in docs:
            if not _is_approved(doc):
                continue
            key = _doc_key(doc)
            if not key:
                continue
            if key not in doc_map:
                doc_map[key] = doc.copy()
                parcel_refs[key] = []
            parcel_refs[key].append(pid)

    to_download: list[dict] = []
    already_cached: list[dict] = []

    for key, doc in doc_map.items():
        book = doc.get("book", "")
        page = doc.get("page", "")
        doc["parcel_ids"] = sorted(set(parcel_refs[key]))
        if book and page:
            (already_cached if scan_exists(book, page) else to_download).append(doc)
        else:
            to_download.append(doc)

    priority_order = ["CX", "TK", "VO", "MD", "RS", "CV"]

    def sort_key(d: dict) -> tuple:
        code = d.get("doc_type_code", "ZZ")
        rank = priority_order.index(code) if code in priority_order else 99
        return (rank, d.get("book", ""), d.get("page", ""))

    to_download.sort(key=sort_key)

    type_counts: dict[str, int] = {}
    for doc in to_download:
        t = doc.get("instrument_type") or doc.get("doc_type_code") or "UNKNOWN"
        type_counts[t] = type_counts.get(t, 0) + 1

    return to_download, {
        "total_approved": len(doc_map),
        "already_cached": len(already_cached),
        "to_download": len(to_download),
        "type_counts": type_counts,
    }


def _build_image_url(doc: dict) -> str | None:
    imid = doc.get("image_id", "")
    if not imid:
        return None
    rec_date = doc.get("recorded_date", "") or doc.get("document_date", "")
    try:
        dt = datetime.strptime(rec_date[:10], "%Y-%m-%d")
        year, month, day = f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}"
    except Exception:
        year = month = day = ""
    ctln = doc.get("document_number", "")
    params = (
        f"WSIQTP=LR01I&W9RCCY={year}&W9RCMM={month}&W9RCDD={day}"
        f"&W9CTLN={ctln}&WSKYCD=B&W9IMID={imid}"
    )
    return f"{REGISTRY_BASE}/ALIS/WW400R.HTM?{params}"


def _extract_image_url_from_viewer(html: bytes) -> str | None:
    text = html.decode("latin-1", errors="replace")
    paths = re.findall(r'/WwwImg/[^\s"\'<>#]+\.PDF', text, re.IGNORECASE)
    if not paths:
        return None
    page_suffix = re.compile(r'\d{4}\.PDF$', re.IGNORECASE)
    base_paths = [p for p in paths if not page_suffix.search(p)]
    chosen = base_paths[0] if base_paths else paths[0]
    return "https://search.barnstabledeeds.org" + chosen


def _try_download_document(rl: RateLimiter, doc: dict, dest: Path) -> tuple[bool, int]:
    url = _build_image_url(doc)
    if not url:
        log.warning("No image URL for book=%s page=%s", doc.get("book"), doc.get("page"))
        return False, 0

    try:
        resp = rl.get(url, stream=True)
    except RegistryThrottleError:
        raise
    except Exception as e:
        log.error("Download request failed: %s", e)
        return False, 0

    if resp.status_code != 200:
        log.warning("HTTP %s downloading %s", resp.status_code, url)
        return False, 0

    content = resp.content
    size = len(content)
    if size < 100:
        log.warning("Suspiciously small response (%d bytes)", size)
        return False, 0

    dest.parent.mkdir(parents=True, exist_ok=True)

    if "text/html" in resp.headers.get("Content-Type", ""):
        real_url = _extract_image_url_from_viewer(content)
        if real_url:
            log.info("Following viewer HTML to: %s", real_url)
            try:
                img_resp = rl.get(real_url, stream=True)
            except Exception as e:
                log.error("Failed to fetch extracted image URL: %s", e)
                (dest.parent / "response.html").write_bytes(content)
                return False, 0
            if img_resp.status_code == 200 and "text/html" not in img_resp.headers.get("Content-Type", ""):
                content = img_resp.content
                size = len(content)
                if size >= 100:
                    dest.write_bytes(content)
                    return True, size
        log.warning("Got HTML instead of PDF — document may require login or cart payment: %s", url)
        (dest.parent / "response.html").write_bytes(content)
        return False, 0

    dest.write_bytes(content)
    return True, size


def download_queue(rl: RateLimiter, queue: list[dict], limit: int) -> dict:
    stats = {"attempted": 0, "succeeded": 0, "failed": 0, "total_bytes": 0, "failures": []}
    registry_dir = _get_config().output_dir("registry")

    for i, doc in enumerate(queue[:limit]):
        book = doc.get("book", "")
        page = doc.get("page", "")
        itype = doc.get("instrument_type") or doc.get("doc_type_code") or "?"

        dest = (scan_path(book, page) if book and page
                else registry_dir / "documents" / "unknown" / doc.get("image_id", "") / "scan.pdf")

        if dest.exists():
            log.info("[%d/%d] SKIP (exists): book=%s page=%s", i + 1, limit, book, page)
            continue

        log.info("[%d/%d] Downloading: book=%s page=%s type=%s", i + 1, limit, book, page, itype)
        stats["attempted"] += 1

        try:
            ok, size = _try_download_document(rl, doc, dest)
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Unexpected error downloading %s/%s: %s", book, page, e)
            stats["failed"] += 1
            stats["failures"].append({"book": book, "page": page, "error": str(e)})
            continue

        if ok:
            stats["succeeded"] += 1
            stats["total_bytes"] += size
            log.info("  → %.1f KB saved", size / 1024)
            meta = dict(doc)
            meta["download_date"] = datetime.now(timezone.utc).isoformat()
            meta["file_size_bytes"] = size
            meta_dest = metadata_path(book, page) if book and page else dest.parent / "metadata.json"
            meta_dest.write_text(json.dumps(meta, indent=2))
        else:
            stats["failed"] += 1
            stats["failures"].append({"book": book, "page": page, "error": "download_failed"})


    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--override-robots", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not check_robots(override=args.override_robots):
        sys.exit(1)

    ensure_cache_dirs()
    log_path = setup_logging("download")
    log.info("Logging to %s", log_path)

    log.info("Building download manifest from all cached indexes...")
    queue, manifest_stats = build_download_manifest()

    print(f"\n{'='*60}")
    print(f"  Total approved instruments:  {manifest_stats.get('total_approved', 0)}")
    print(f"  Already cached (skip):       {manifest_stats.get('already_cached', 0)}")
    print(f"  Queued for download:         {manifest_stats.get('to_download', 0)}")
    print()
    for t, c in sorted(manifest_stats.get("type_counts", {}).items(), key=lambda x: -x[1]):
        print(f"    {c:4d}  {t}")
    print(f"{'='*60}\n")

    if not queue:
        print("Nothing to download.")
        return

    limit = args.limit if args.limit > 0 else len(queue)

    if not args.confirm:
        reply = input(f"Download {min(limit, len(queue))} documents? [y/N] ").strip().lower()
        if reply != "y":
            print("Aborted.")
            return

    rl = RateLimiter()
    dl_stats: dict = {}
    try:
        dl_stats = download_queue(rl, queue, limit)
    finally:
        rl.close()

    lines = [
        "Registry Download Report",
        f"Generated: {datetime.now().isoformat()}",
        f"Attempted:   {dl_stats.get('attempted', 0)}",
        f"Succeeded:   {dl_stats.get('succeeded', 0)}",
        f"Failed:      {dl_stats.get('failed', 0)}",
        f"Total size:  {dl_stats.get('total_bytes', 0) / 1024 / 1024:.1f} MB",
    ]
    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text("\n".join(lines))

    print(f"Downloaded: {dl_stats.get('succeeded', 0)}")
    print(f"Failed:     {dl_stats.get('failed', 0)}")
    print(f"Total size: {dl_stats.get('total_bytes', 0) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()

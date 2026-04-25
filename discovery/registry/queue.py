"""
Discovery — Build Registry target queue from the database.

Reads parcels from the main SQLite database (requires processing pipeline to
have run first). Writes queue to CivicTwin/registry/queue/target_queue.csv.

Run before discovery.registry.enumerate.

Usage:
    python3 -m discovery.registry.queue            # priority parcels only
    python3 -m discovery.registry.queue --full     # all unenumerated parcels
"""

import argparse
import csv
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from discovery.config import get_config, LOCAL_OUTPUT_DIR

CURRENT_YEAR = datetime.now().year
REPORT_TXT = LOCAL_OUTPUT_DIR / "registry_queue_report.txt"

ARTICLE97_DOC_TYPES = "DD,TK,CX,RS,CV,VO,CO,FD,DD/LC,DL/TR,AG"
CCR_DOC_TYPES       = "MD,RS,CV,CX,AR,AG,DL/TR,A/MD"
BOTH_DOC_TYPES      = "DD,TK,CX,RS,CV,VO,CO,FD,DD/LC,MD,AR,AG,DL/TR,A/MD"

_PUNCT     = re.compile(r"[^\w\s]")
_MULTI_SPC = re.compile(r"\s{2,}")
_ORG_KW    = re.compile(
    r"\b(LLC|INC|CORP|LTD|LLP|LP|ASSOCIATION|ASSOC|COMMISSION|COMMITTEE|"
    r"REALTY|PROPERTIES|PROPERTY|TRUST|FOUNDATION|CLUB|CONDO|CONDOMINIUM|"
    r"COMPANY|ENTERPRISES|DEVELOPMENT|CAPITAL|ENERGY|GROUP|PARTNERS|"
    r"BANK|SAVINGS|MORTGAGE|TOWN|CITY|COUNTY|STATE|COMMONWEALTH|"
    r"CONSERVATION|LAND TRUST|DISTRICT|AUTHORITY)\b",
    re.IGNORECASE,
)
_PERSON_SFX = re.compile(
    r"\s+(?:TRUSTEE|TR|TTEE|EXECUTOR|EXECUTRIX|ET UX|ET VIR|ET AL[I]?|&?\s*W|&\s*H|"
    r"JR|SR|II|III|IV|V)\s*$",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _clean(raw: str) -> str:
    n = str(raw).upper().strip()
    n = _PUNCT.sub(" ", n)
    return _MULTI_SPC.sub(" ", n).strip()


def _build_search_name(owner_name: str) -> tuple[str, str, str, str]:
    cleaned = _clean(owner_name)
    if "TOWN OF DENNIS" in cleaned or "SELECTMEN" in cleaned or "SELECTBOARD" in cleaned:
        return "DENNIS", "", "org", "TOWN OF DENNIS → DENNIS"
    if "HOMEOWNERS" in cleaned or "HOME OWNERS" in cleaned or " HOA" in cleaned:
        m = re.match(
            r"^(.+?)\s+(?:HOMEOWNERS?\s*(?:ASSOC\w*)?|HOME\s+OWNERS\s+(?:ASSOC\w*)?|HOA\b)",
            cleaned, re.IGNORECASE,
        )
        if m:
            sub = m.group(1).strip()
            return sub, "", "org", f"HOA subdivision: {sub}"
        return cleaned, "", "org", "HOA no subdivision"
    if _ORG_KW.search(cleaned):
        return cleaned, "", "org", "organization"
    stripped = _PERSON_SFX.sub("", cleaned).strip()
    parts = stripped.split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1], "individual", f"individual: {parts[0]} / {parts[1]}"
    return stripped, "", "individual", f"single-word: {stripped}"


def _is_land_court(deed_book: str) -> bool:
    b = str(deed_book).upper().strip()
    return b.startswith("LC") or "LAND COURT" in b


def _queue_path() -> Path:
    return get_config().output_dir("registry") / "queue" / "target_queue.csv"


def _db_path() -> Path:
    return get_config().db_path("dennis")


def build_queue(con: sqlite3.Connection) -> tuple[list[dict], list[str]]:
    report: list[str] = []

    rows = con.execute("""
        SELECT
            p.parcel_id,
            p.site_addr         AS site_address,
            p.owner_name,
            p.booklast          AS deed_book,
            p.pagelast          AS deed_page,
            COALESCE(p.article_97_priority, 0)  AS a97_score,
            COALESCE(p.article_97_reasons, '')  AS a97_reasons,
            COALESCE(p.ccr_priority, 0)         AS ccr_score,
            COALESCE(p.ccr_reasons, '')         AS ccr_reasons,
            MIN(w.meeting_date)                 AS earliest_meeting_date
        FROM parcels p
        LEFT JOIN parcel_warrant_links pwl ON p.parcel_id = pwl.parcel_id
        LEFT JOIN warrants w ON pwl.warrant_id = w.warrant_id
        WHERE p.article_97_priority >= 40
           OR p.ccr_priority >= 40
           OR p.article_97_priority >= 25
           OR p.use_code IN (
               '0130','0131','0170','0370','0310',
               '7160','7170','9380','9390'
           )
        GROUP BY p.parcel_id
        ORDER BY MAX(COALESCE(p.article_97_priority,0), COALESCE(p.ccr_priority,0)) DESC
    """).fetchall()

    cols = ["parcel_id", "site_address", "owner_name", "deed_book", "deed_page",
            "a97_score", "a97_reasons", "ccr_score", "ccr_reasons", "earliest_meeting_date"]

    rd_grantors: dict[str, str] = {}
    try:
        for row in con.execute(
            "SELECT parcel_id, grantor FROM registry_documents WHERE doc_rank = 1"
        ).fetchall():
            if row[1]:
                rd_grantors[row[0]] = row[1]
    except Exception:
        pass

    queue_rows: list[dict] = []
    excluded: list[str] = []
    name_decisions: list[str] = []

    for row in rows:
        r = dict(zip(cols, row))
        pid = r["parcel_id"]
        if not r["owner_name"]:
            excluded.append(f"{pid}: no owner name")
            continue

        search_last, search_first, name_type, note = _build_search_name(r["owner_name"])
        if not search_last:
            excluded.append(f"{pid}: no search name from '{r['owner_name']}'")
            continue

        name_decisions.append(
            f"  {pid:15s}  '{r['owner_name']}' → W9SNM='{search_last}' W9GNM='{search_first}'  [{note}]"
        )

        date_start = "1970"
        if r["earliest_meeting_date"]:
            try:
                acq_year = int(str(r["earliest_meeting_date"])[:4])
                date_start = str(max(acq_year - 2, 1742))
            except ValueError:
                pass

        in_a97 = int(r["a97_score"]) >= 40
        in_ccr = int(r["ccr_score"]) >= 40
        if in_a97 and in_ccr:
            project, doc_types = "BOTH", BOTH_DOC_TYPES
        elif in_a97:
            project, doc_types = "ARTICLE97", ARTICLE97_DOC_TYPES
        else:
            project, doc_types = "CCR", CCR_DOC_TYPES

        priority_score = max(int(r["a97_score"]), int(r["ccr_score"]))
        reasons = set()
        if r["a97_reasons"]:
            reasons.update(r["a97_reasons"].split(","))
        if r["ccr_reasons"]:
            reasons.update(r["ccr_reasons"].split(","))

        queue_rows.append({
            "parcel_id": pid, "project": project,
            "site_address": r["site_address"] or "",
            "owner_name": r["owner_name"],
            "search_name_primary": search_last,
            "search_name_first": search_first,
            "name_type": name_type,
            "search_name_secondary": rd_grantors.get(pid, ""),
            "search_date_start": date_start,
            "search_date_end": str(CURRENT_YEAR),
            "document_types_of_interest": doc_types,
            "priority_score": priority_score,
            "reason_codes": ",".join(sorted(reasons)),
            "is_land_court": str(_is_land_court(r["deed_book"] or "")),
            "deed_book": r["deed_book"] or "",
            "deed_page": r["deed_page"] or "",
        })

    queue_rows.sort(key=lambda r: int(r["priority_score"]), reverse=True)

    report.append(f"Total parcels in queue: {len(queue_rows)}")
    report.append(f"Excluded: {len(excluded)}")
    report.append("")
    report.append("=== Name Cleaning Decisions (first 50) ===")
    report.extend(name_decisions[:50])
    if len(name_decisions) > 50:
        report.append(f"  ... and {len(name_decisions) - 50} more")

    return queue_rows, report


def build_full_queue(con: sqlite3.Connection) -> tuple[list[dict], list[str]]:
    """Build a queue of every parcel not yet in the registry index."""
    report: list[str] = []

    enumerated: set[str] = set()
    try:
        for row in con.execute("SELECT DISTINCT parcel_id FROM registry_documents"):
            enumerated.add(row[0])
    except Exception:
        pass
    report.append(f"Already enumerated: {len(enumerated)}")

    rows = con.execute("""
        SELECT
            p.parcel_id,
            MAX(p.site_addr)      AS site_address,
            MAX(p.owner_name)     AS owner_name,
            MAX(p.booklast)       AS deed_book,
            MAX(p.pagelast)       AS deed_page,
            MAX(p.use_code)       AS use_code
        FROM parcels p
        GROUP BY p.parcel_id
        ORDER BY p.parcel_id
    """).fetchall()

    cols = ["parcel_id", "site_address", "owner_name", "deed_book", "deed_page", "use_code"]

    rd_grantors: dict[str, str] = {}
    try:
        for row in con.execute(
            "SELECT parcel_id, grantor FROM registry_documents WHERE doc_rank = 1"
        ).fetchall():
            if row[1]:
                rd_grantors[row[0]] = row[1]
    except Exception:
        pass

    def _tier(use_code: str) -> int:
        if not use_code:
            return 6
        c = use_code.strip()
        if c.startswith("9"):
            return 1
        if c.startswith("01") or c.startswith("02") or c.startswith("06") or c.startswith("03"):
            return 2
        if c.startswith("3") or c.startswith("4") or c.startswith("5"):
            return 3
        if c == "1010":
            return 4
        if c in ("1020", "1021", "1023", "1320"):
            return 5
        return 4

    queue_rows: list[dict] = []
    excluded: list[str] = []

    for row in rows:
        r = dict(zip(cols, row))
        pid = r["parcel_id"]
        if pid in enumerated:
            continue
        if not r["owner_name"]:
            excluded.append(f"{pid}: no owner name")
            continue

        search_last, search_first, name_type, _ = _build_search_name(r["owner_name"])
        if not search_last:
            excluded.append(f"{pid}: no search name")
            continue

        queue_rows.append({
            "parcel_id": pid, "project": "SCAN",
            "site_address": r["site_address"] or "",
            "owner_name": r["owner_name"],
            "search_name_primary": search_last,
            "search_name_first": search_first,
            "name_type": name_type,
            "search_name_secondary": rd_grantors.get(pid, ""),
            "search_date_start": "1742",
            "search_date_end": str(CURRENT_YEAR),
            "document_types_of_interest": "ALL",
            "priority_score": _tier(r["use_code"] or ""),
            "reason_codes": r["use_code"] or "",
            "is_land_court": str(_is_land_court(r["deed_book"] or "")),
            "deed_book": r["deed_book"] or "",
            "deed_page": r["deed_page"] or "",
        })

    queue_rows.sort(key=lambda r: (int(r["priority_score"]), r["site_address"]))
    report.append(f"Full queue: {len(queue_rows)} parcels  (excluded {len(excluded)})")
    return queue_rows, report


QUEUE_FIELDNAMES = [
    "parcel_id", "project", "site_address", "owner_name",
    "search_name_primary", "search_name_first", "name_type",
    "search_name_secondary", "search_date_start", "search_date_end",
    "document_types_of_interest", "priority_score", "reason_codes",
    "is_land_court", "deed_book", "deed_page",
]


def write_queue(queue_rows: list[dict]) -> Path:
    queue_csv = _queue_path()
    queue_csv.parent.mkdir(parents=True, exist_ok=True)
    with queue_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=QUEUE_FIELDNAMES)
        w.writeheader()
        w.writerows(queue_rows)
    log.info("Queue: %d rows → %s", len(queue_rows), queue_csv)
    return queue_csv


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--full", action="store_true",
                        help="Queue every unenumerated parcel, not just priority ones")
    args = parser.parse_args()

    db_path = _db_path()
    if not db_path.exists():
        log.error("Database not found: %s — run processing pipeline first.", db_path)
        sys.exit(1)

    con = sqlite3.connect(db_path)
    try:
        if args.full:
            queue_rows, report_lines = build_full_queue(con)
        else:
            queue_rows, report_lines = build_queue(con)
    finally:
        con.close()

    queue_csv = write_queue(queue_rows)

    mode = "full" if args.full else "priority"
    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text("\n".join([
        f"Registry Queue Report ({mode})",
        f"Generated: {datetime.now().isoformat()}",
        f"Queue size: {len(queue_rows)}", "",
        *report_lines,
    ]))

    tier1 = sum(1 for r in queue_rows if r["deed_book"])
    tier2 = len(queue_rows) - tier1
    print(f"\nQueue ({mode}): {len(queue_rows)} parcels")
    print(f"  Tier 1 (book/page):   {tier1}")
    print(f"  Tier 2 (name search): {tier2}")
    print(f"Queue file: {queue_csv}")


if __name__ == "__main__":
    main()

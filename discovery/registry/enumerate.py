"""Registry enumerate pass — Book/Page lookup (Tier 1) and name search (Tier 2).

Reads the target queue CSV and resolves each parcel's deed via a direct
Book/Page lookup (Tier 1, parcels with a deed reference) or by owner name
(Tier 2, parcels without a deed reference).

Usage:
    python3 -m discovery.registry.enumerate [--override-robots] [--tier2]
                                            [--limit N] [--start-after PARCEL_ID]
"""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from discovery.config import get_config, LOCAL_OUTPUT_DIR
from discovery.registry.cache import (
    ensure_cache_dirs,
    get_cached_index,
    save_index,
)
from discovery.registry.ratelimit import RateLimiter, RegistryThrottleError, check_robots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

REGISTRY_BASE = "https://search.barnstabledeeds.org"
SEARCH_URL = f"{REGISTRY_BASE}/ALIS/WW400R.HTM"

REPORT_TXT = LOCAL_OUTPUT_DIR / "registry_enumerate_report.txt"


def _queue_csv() -> Path:
    return get_config().output_dir("registry") / "queue" / "target_queue.csv"


# ── Book/Page lookup ──────────────────────────────────────────────────────────

def _bp_lookup_params(book: str, page: str) -> dict:
    return {
        "WSHTNM": "WW409R00", "WSIQTP": "LR09AP", "WSKYCD": "B", "WSWVER": "2",
        "W9BK": str(book).strip(), "W9PG": str(page).strip(),
    }


def _parse_bp_result(html: str, parcel_id: str, project: str) -> dict | None:
    if "No records found" in html or "no records" in html.lower():
        return None
    if "Bk-Pg:" not in html:
        return None

    def _txt(pattern: str, default: str = "") -> str:
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        return m.group(1).strip() if m else default

    def _strip(s: str) -> str:
        return re.sub(r"<[^>]+>", "", s).strip()

    bk_pg = _txt(r"Bk-Pg:([\d\-]+)")
    recorded_raw = _txt(r"Recorded:\s*([\d\-]+)")
    try:
        recorded_date = datetime.strptime(recorded_raw, "%m-%d-%Y").strftime("%Y-%m-%d")
    except ValueError:
        recorded_date = recorded_raw

    inst_number = _txt(r"Inst #:\s*(\d+)")
    ctln  = _txt(r'name="W9CTLN"[^>]*value="([^"]+)"')
    rccy  = _txt(r'name="W9RCCY"[^>]*value="([^"]+)"')
    rcmm  = _txt(r'name="W9RCMM"[^>]*value="([^"]+)"')
    rcdd  = _txt(r'name="W9RCDD"[^>]*value="([^"]+)"')
    imid  = _txt(r'W9IMID=([^\s"\'&]+)')
    pages = _txt(r"Pages in document:\s*(\d+)")
    inst_type  = _txt(r"Type:(?:&#160;|&nbsp;|\s)+([A-Za-z/ ]+?)(?:&#160;|&nbsp;|Doc\$|<|\s{2,})")
    doc_amount = _txt(r"Doc\$:\s*([\d,\.]+)")
    description = _txt(r"Desc:\s*([^\n<]+)")
    town    = _txt(r"Town:\s*([A-Z]+)")
    address = _txt(r"Addr:\s*([^\n<\&]+)")

    grantors = re.findall(r'<td[^>]*>.*?Gtor:.*?<a[^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE)
    grantees = re.findall(r'<td[^>]*>.*?Gtee:.*?<a[^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE)

    def _role_strip(s: str) -> str:
        return re.sub(r"\s*\((Gtor|Gtee)\)\s*$", "", _strip(s), flags=re.IGNORECASE).strip()

    grantor_str = "; ".join(_role_strip(g) for g in grantors) if grantors else ""
    grantee_str = "; ".join(_role_strip(g) for g in grantees) if grantees else ""
    xrefs = re.findall(r'href="[^"]*WSIQTP=LR09A[^"]*W9BK=(\d+)[^"]*W9PG=(\d+)"', html)
    cross_refs = [f"{b.lstrip('0')}-{p.lstrip('0')}" for b, p in xrefs]

    book_num, page_num = ("", "")
    if "-" in bk_pg:
        parts = bk_pg.split("-", 1)
        book_num, page_num = parts[0].strip(), parts[1].strip()

    return {
        "parcel_id": parcel_id, "search_name": f"book/{page_num}",
        "lookup_method": "book_page", "instrument_type": inst_type.strip(),
        "doc_type_code": "", "document_date": recorded_date, "recorded_date": recorded_date,
        "grantor": grantor_str, "grantee": grantee_str, "town": town, "address": address,
        "book": book_num, "page": page_num,
        "document_number": inst_number or ctln.lstrip("0"),
        "image_id": imid, "ctln": ctln, "rccy": rccy, "rcmm": rcmm, "rcdd": rcdd,
        "pages_in_doc": pages, "doc_amount": doc_amount,
        "description": description.strip(), "cross_refs": cross_refs, "relevance": project,
    }


def lookup_book_page(rl: RateLimiter, book: str, page: str,
                     parcel_id: str, project: str) -> dict | None:
    params = _bp_lookup_params(book, page)
    try:
        resp = rl.get(SEARCH_URL, params=params)
    except RegistryThrottleError:
        raise
    except Exception as e:
        log.error("Book/page request failed for %s (book=%s page=%s): %s", parcel_id, book, page, e)
        return None

    if resp.status_code != 200:
        log.warning("HTTP %s for %s book=%s page=%s", resp.status_code, parcel_id, book, page)
        return None

    return _parse_bp_result(resp.text, parcel_id, project)


# ── Name search (Tier 2) ──────────────────────────────────────────────────────

MAX_PAGES = 5
RESULTS_PER_PAGE = "30"
_TR = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")
_IMG_RE = re.compile(
    r'WSIQTP=LR01I[^"\']*&W9RCCY=(\d+)&W9RCMM=(\d+)&W9RCDD=(\d+)'
    r'&W9CTLN=(\d+)&WSKYCD=B&W9IMID=([^"\'&\s]+)', re.IGNORECASE)


def _name_search_params(search_name: str, search_first: str,
                        is_lc: bool, date_start: str, date_end: str,
                        direction: str = "A") -> dict:
    try:
        fdta = f"0101{int(date_start):04d}"
        tdta = f"1231{int(date_end):04d}"
    except ValueError:
        fdta, tdta = "", ""

    if is_lc:
        return {"WSHTNM": "WW401L00", "WSIQTP": "LC01LP", "WSWVER": "2",
                "W9SN8": search_name, "W9GN8": search_first,
                "W9IXTP": direction, "W9ABR": "*ALL", "W9TOWN": "DENN",
                "W9FDTA": fdta, "W9TDTA": tdta, "WSSRPP": RESULTS_PER_PAGE}
    return {"WSHTNM": "WW401R00", "WSIQTP": "LR01LP", "WSKYCD": "N", "WSWVER": "2",
            "W9SNM": search_name, "W9GNM": search_first,
            "W9IXTP": direction, "W9ABR": "*ALL", "W9TOWN": "DENN",
            "W9FDTA": fdta, "W9TDTA": tdta, "WSSRPP": RESULTS_PER_PAGE}


def _parse_name_results(html: str, parcel_id: str, search_name: str,
                        project: str) -> list[dict]:
    docs = []
    tbl = re.search(r"<table[^>]*>.*?<th>Name</th>.*?</table>", html, re.DOTALL | re.IGNORECASE)
    if not tbl:
        return docs

    for row_html in _TR.findall(tbl.group(0)):
        cells = [_TAG.sub("", td).strip() for td in _TD.findall(row_html)]
        if len(cells) < 7 or cells[0].lower() in ("name", ""):
            continue

        name_cell, reverse, town, date_recv = cells[0], cells[1], cells[2], cells[3]
        doc_type, doc_desc, book_page_raw = cells[4], cells[5], cells[6]

        try:
            recorded = datetime.strptime(date_recv.strip(), "%m-%d-%Y").strftime("%Y-%m-%d")
        except ValueError:
            recorded = date_recv.strip()

        book, page = ("", "")
        if book_page_raw.upper() not in ("SEE INSTRUMENT", "", "N/A"):
            for sep in ("-", "/"):
                if sep in book_page_raw:
                    parts = book_page_raw.split(sep, 1)
                    book, page = parts[0].strip(), parts[1].strip()
                    break

        role = re.search(r"\((Gtee|Gtor)\)", name_cell, re.IGNORECASE)
        role_str = role.group(1).upper() if role else ""
        is_grantee = "GTEE" in role_str

        img_m = _IMG_RE.search(row_html)
        imid = img_m.group(5) if img_m else ""
        ctln = img_m.group(4) if img_m else ""

        docs.append({
            "parcel_id": parcel_id, "search_name": search_name,
            "lookup_method": "name_search",
            "instrument_type": doc_desc.strip() or doc_type.strip(),
            "doc_type_code": doc_type.strip(),
            "document_date": recorded, "recorded_date": recorded,
            "grantor": "" if is_grantee else name_cell.strip(),
            "grantee": name_cell.strip() if is_grantee else reverse,
            "reverse_party": reverse, "town": town,
            "book": book, "page": page, "document_number": ctln, "image_id": imid,
            "description": doc_desc.strip(), "relevance": project,
        })
    return docs


def _has_next(html: str) -> bool:
    return bool(re.search(r'class="nextPage"[^>]*>Next</a>', html, re.IGNORECASE))


def name_search(rl: RateLimiter, search_name: str, search_first: str,
                is_lc: bool, date_start: str, date_end: str,
                parcel_id: str, project: str,
                direction: str = "A") -> list[dict]:
    if not search_name.strip():
        return []
    params = _name_search_params(search_name, search_first, is_lc, date_start, date_end,
                                 direction)
    all_docs = []
    next_iqtp = "LR01N" if not is_lc else "LC01N"

    for page_num in range(1, MAX_PAGES + 1):
        if page_num > 1:
            params["WSIQTP"] = next_iqtp
        try:
            resp = rl.get(SEARCH_URL, params=params)
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Name search failed for %s '%s': %s", parcel_id, search_name, e)
            break

        if resp.status_code != 200:
            break

        page_docs = _parse_name_results(resp.text, parcel_id, search_name, project)
        all_docs.extend(page_docs)
        if not _has_next(resp.text) or not page_docs:
            break
        if page_num == MAX_PAGES:
            log.warning("Page cap hit for %s '%s' — results truncated", parcel_id, search_name)

    return all_docs


def _dedup(docs: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for d in docs:
        key = f"{d['book']}-{d['page']}" if d.get('book') else d.get('image_id', '')
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out


# ── Processing ────────────────────────────────────────────────────────────────

def process_tier1(rl: RateLimiter, queue: list[dict],
                  start_after: str, limit: int) -> dict:
    tier1 = [r for r in queue if r.get("deed_book", "").strip()
             and r["deed_book"].strip() not in ("", "0")]

    stats = {"total": len(tier1), "attempted": 0, "cache_hits": 0,
             "succeeded": 0, "no_result": 0, "errors": 0, "zero_result_pids": []}
    skipping = bool(start_after)

    for row in tier1:
        pid = row["parcel_id"]
        if skipping:
            if pid == start_after:
                skipping = False
            continue
        if limit and stats["attempted"] >= limit:
            log.info("Reached --limit %d, stopping.", limit)
            break

        cached = get_cached_index(pid)
        if cached is not None:
            log.info("CACHE HIT   %s (%d docs)", pid, len(cached))
            stats["cache_hits"] += 1
            continue

        book = row["deed_book"].strip()
        page = row["deed_page"].strip()
        project = row["project"]
        log.info("LOOKUP      %s  book=%s page=%s", pid, book, page)
        stats["attempted"] += 1

        try:
            doc = lookup_book_page(rl, book, page, pid, project)
        except RegistryThrottleError:
            raise

        if doc is None:
            log.warning("  → no result for %s", pid)
            stats["no_result"] += 1
            stats["zero_result_pids"].append(pid)
            save_index(pid, [])
        else:
            log.info("  → %s / %s — %s", doc["grantor"], doc["grantee"], doc["instrument_type"])
            save_index(pid, [doc])
            stats["succeeded"] += 1

    return stats


def process_tier2(rl: RateLimiter, queue: list[dict], limit: int) -> dict:
    no_deed = [r for r in queue if not r.get("deed_book", "").strip()
               or r["deed_book"].strip() in ("", "0")]

    BROAD_NAMES = {"DENNIS"}
    runnable = [r for r in no_deed if r["search_name_primary"] not in BROAD_NAMES]
    deferred = [r for r in no_deed if r["search_name_primary"] in BROAD_NAMES]

    log.info("Tier 2: %d parcels to search, %d deferred (broad names)",
             len(runnable), len(deferred))

    stats = {"total": len(runnable), "deferred": len(deferred), "attempted": 0,
             "cache_hits": 0, "succeeded": 0, "zero_result_pids": [], "errors": 0}

    for row in runnable:
        pid = row["parcel_id"]
        if limit and stats["attempted"] >= limit:
            break

        cached = get_cached_index(pid)
        if cached is not None:
            stats["cache_hits"] += 1
            continue

        stats["attempted"] += 1
        is_lc = row.get("is_land_court", "False") == "True"
        project = row["project"]
        primary = row["search_name_primary"]
        first = row.get("search_name_first", "")
        secondary = row.get("search_name_secondary", "")
        date_start = row.get("search_date_start", "1970")
        date_end = row.get("search_date_end", str(datetime.now().year))

        log.info("NAME SEARCH %s  W9SNM='%s' W9GNM='%s'", pid, primary, first)

        try:
            docs = name_search(rl, primary, first, is_lc, date_start, date_end, pid, project)
            if secondary and secondary != primary:
                docs += name_search(rl, secondary, "", is_lc, date_start, date_end, pid, project)
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Error searching %s: %s", pid, e)
            stats["errors"] += 1
            continue

        unique = _dedup(docs)
        save_index(pid, unique)
        if len(unique) == 0:
            stats["zero_result_pids"].append(pid)
            log.warning("  → zero results for %s", pid)
        else:
            log.info("  → %d documents", len(unique))
        stats["succeeded"] += 1

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--override-robots", action="store_true")
    parser.add_argument("--tier2", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--start-after", default="")
    args = parser.parse_args()

    if not check_robots(override=args.override_robots):
        sys.exit(1)

    ensure_cache_dirs()

    queue_csv = _queue_csv()
    if not queue_csv.exists():
        log.error("Queue not found: %s — run discovery.registry.queue first.", queue_csv)
        sys.exit(1)

    with queue_csv.open(newline="", encoding="utf-8") as f:
        queue = list(csv.DictReader(f))
    log.info("Loaded %d parcels from queue", len(queue))

    rl = RateLimiter()
    t1_stats, t2_stats = {}, None

    try:
        t1_stats = process_tier1(rl, queue, args.start_after, args.limit)
        if args.tier2:
            t2_stats = process_tier2(rl, queue, args.limit)
    except RegistryThrottleError as e:
        log.error("STOPPED: %s", e)
        sys.exit(1)
    finally:
        rl.close()

    # Write report
    from discovery.registry.cache import all_cached_indexes
    all_cached = all_cached_indexes()
    lines = [
        "Registry Enumerate Report",
        f"Generated: {datetime.now().isoformat()}",
        "", "=== Tier 1: Book/Page Lookups ===",
        f"Parcels eligible:   {t1_stats.get('total', 0)}",
        f"Cache hits:         {t1_stats.get('cache_hits', 0)}",
        f"Network lookups:    {t1_stats.get('attempted', 0)}",
        f"Succeeded:          {t1_stats.get('succeeded', 0)}",
        f"No result:          {t1_stats.get('no_result', 0)}",
    ]
    if t2_stats:
        lines += [
            "", "=== Tier 2: Name Searches ===",
            f"Parcels run:        {t2_stats.get('total', 0)}",
            f"Deferred (broad):   {t2_stats.get('deferred', 0)}",
            f"Cache hits:         {t2_stats.get('cache_hits', 0)}",
            f"Network searches:   {t2_stats.get('attempted', 0)}",
            f"Succeeded:          {t2_stats.get('succeeded', 0)}",
        ]

    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text("\n".join(lines))
    log.info("Report: %s", REPORT_TXT)


if __name__ == "__main__":
    main()

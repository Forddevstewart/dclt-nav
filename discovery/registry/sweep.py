"""Registry sweep — cross-reference expansion and Town of Dennis name sweep.

Two supplemental passes that recover documents missed by the main enumerate pass:

  --xrefs       Cross-reference expansion: looks up book/page pairs referenced
                in existing index documents but not yet indexed themselves.

  --town-sweep  Date-windowed name searches for "DENNIS" as both grantor and
                grantee, recovering Town-recorded documents (takings, conservation
                restrictions, certificates of vote, etc.).

Run after the main enumerate pass completes. Safe to re-run; already-cached
windows are skipped (30-day staleness window, same as the main enumerate pass).

Usage:
    python3 -m discovery.registry.sweep [--override-robots]
                                        [--xrefs] [--town-sweep]
                                        [--limit N]
"""

import argparse
import calendar
import logging
import sys
from datetime import datetime

from discovery.config import LOCAL_OUTPUT_DIR
from discovery.registry.cache import (
    ensure_cache_dirs,
    all_cached_indexes,
    get_cached_index,
    save_index,
    scan_exists,
    setup_logging,
)
from discovery.registry.enumerate import lookup_book_page, name_search, _dedup
from discovery.registry.ratelimit import RateLimiter, RegistryThrottleError, check_robots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

REPORT_TXT = LOCAL_OUTPUT_DIR / "registry_sweep_report.txt"

TOWN_SEARCH_NAME = "DENNIS TOWN"   # prefix-matches "DENNIS TOWN OF", "DENNIS TOWN OF (CONSERVATION)", etc.
TOWN_SEARCH_FIRST = ""
DIRECTIONS = [("G", "grantor"), ("E", "grantee")]
SWEEP_MAX_PAGES = 20  # deeper paging for broad name searches


def _date_windows() -> list[tuple[str, str]]:
    current_year = datetime.now().year
    windows = []
    for y in range(1793, 1950, 10):   # Dennis incorporated 1793; no town records predate this
        windows.append((str(y), str(min(y + 9, 1949))))
    for y in range(1950, current_year + 1, 5):
        windows.append((str(y), str(min(y + 4, current_year))))
    return windows


# ── Option 2: Cross-reference expansion ──────────────────────────────────────

def _all_indexed_bp() -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for _, docs in all_cached_indexes():
        for d in docs:
            b = d.get("book", "").strip()
            p = d.get("page", "").strip()
            if b and p:
                pairs.add((b, p))
    return pairs


def collect_xref_targets() -> list[tuple[str, str]]:
    indexed = _all_indexed_bp()
    targets: set[tuple[str, str]] = set()
    for _, docs in all_cached_indexes():
        for d in docs:
            for ref in d.get("cross_refs", []):
                if "-" not in ref:
                    continue
                b, p = ref.split("-", 1)
                b, p = b.strip(), p.strip()
                if b and p and (b, p) not in indexed and not scan_exists(b, p):
                    targets.add((b, p))
    return sorted(targets)


def process_xrefs(rl: RateLimiter, targets: list[tuple[str, str]], limit: int) -> dict:
    stats = {"total": len(targets), "attempted": 0, "cache_hits": 0,
             "succeeded": 0, "no_result": 0, "errors": 0}

    for book, page in (targets[:limit] if limit else targets):
        pid = f"xref-{book}-{page}"

        if get_cached_index(pid) is not None:
            stats["cache_hits"] += 1
            continue

        stats["attempted"] += 1
        log.info("XREF        book=%s page=%s", book, page)

        try:
            doc = lookup_book_page(rl, book, page, pid, "XREF")
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Xref lookup failed %s/%s: %s", book, page, e)
            stats["errors"] += 1
            continue

        if doc is None:
            log.info("  → no result")
            stats["no_result"] += 1
            save_index(pid, [])
        else:
            log.info("  → %s — %s", doc.get("instrument_type", "?"), doc.get("grantor", "?"))
            save_index(pid, [doc])
            stats["succeeded"] += 1

    return stats


# ── Option 1: Town of Dennis date-windowed sweep ──────────────────────────────

def _sweep_window(rl: RateLimiter, direction: str, dir_label: str,
                  year_start: str, year_end: str,
                  pid: str, stats: dict, limit: int) -> tuple[list[dict], bool]:
    """Fetch one window; subdivide year-by-year if the page cap is hit."""
    # If a prior run subdivided this window but was killed before saving the parent,
    # sub-year caches already exist — skip the network call and reconstruct directly.
    first_sub_pid = f"sweep-denn-{direction}-{year_start}-{year_start}"
    if year_start != year_end and get_cached_index(first_sub_pid) is not None:
        log.info("TOWN SWEEP  %s %s–%s (reconstructing from sub-windows)", dir_label, year_start, year_end)
        return _collect_sub_years(rl, direction, dir_label, year_start, year_end, stats, limit)

    if limit and stats["attempted"] >= limit:
        log.info("Reached --limit %d, stopping.", limit)
        raise StopIteration

    stats["attempted"] += 1
    log.info("TOWN SWEEP  %s %s–%s", dir_label, year_start, year_end)

    try:
        docs, truncated = name_search(
            rl, TOWN_SEARCH_NAME, TOWN_SEARCH_FIRST, False,
            year_start, year_end,
            pid, "TOWN-SWEEP",
            direction=direction,
            max_pages=SWEEP_MAX_PAGES,
        )
    except RegistryThrottleError:
        raise

    if not truncated or year_start == year_end:
        return docs, truncated

    log.info("  Subdividing %s–%s into 1-year windows to recover truncated results",
             year_start, year_end)
    return _collect_sub_years(rl, direction, dir_label, year_start, year_end, stats, limit)


def _collect_sub_months(rl: RateLimiter, direction: str, dir_label: str,
                        year: int, stats: dict, limit: int) -> tuple[list[dict], bool]:
    all_docs: list[dict] = []
    any_truncated = False
    for mm in range(1, 13):
        sub_pid = f"sweep-denn-{direction}-{year}-{mm:02d}"
        cached = get_cached_index(sub_pid)
        if cached is not None:
            all_docs.extend(cached)
            stats["cache_hits"] += 1
            continue

        if limit and stats["attempted"] >= limit:
            log.info("Reached --limit %d, stopping.", limit)
            break

        stats["attempted"] += 1
        _, last_day = calendar.monthrange(year, mm)
        fdta = f"{mm:02d}01{year:04d}"
        tdta = f"{mm:02d}{last_day:02d}{year:04d}"
        log.info("TOWN SWEEP  %s %d-%02d (monthly sub-window)", dir_label, year, mm)
        try:
            sub_docs, sub_trunc = name_search(
                rl, TOWN_SEARCH_NAME, TOWN_SEARCH_FIRST, False,
                str(year), str(year),
                sub_pid, "TOWN-SWEEP",
                direction=direction,
                fdta=fdta, tdta=tdta,
                max_pages=SWEEP_MAX_PAGES,
            )
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Town sweep monthly sub-window error %s %d-%02d: %s", direction, year, mm, e)
            stats["errors"] += 1
            continue

        if sub_trunc:
            log.warning("Page cap hit even for month %d-%02d %s — results still truncated",
                        year, mm, direction)
            any_truncated = True
        sub_unique = _dedup(sub_docs)
        save_index(sub_pid, sub_unique, truncated=sub_trunc)
        all_docs.extend(sub_unique)
        log.info("  → %d documents", len(sub_unique))

    return all_docs, any_truncated


def _collect_sub_years(rl: RateLimiter, direction: str, dir_label: str,
                       year_start: str, year_end: str,
                       stats: dict, limit: int) -> tuple[list[dict], bool]:
    all_docs: list[dict] = []
    any_truncated = False
    for y in range(int(year_start), int(year_end) + 1):
        sub_pid = f"sweep-denn-{direction}-{y}-{y}"
        cached = get_cached_index(sub_pid)
        if cached is not None:
            all_docs.extend(cached)
            stats["cache_hits"] += 1
            continue

        # If the first monthly pid exists, a previous run already subdivided this year.
        first_month_pid = f"sweep-denn-{direction}-{y}-01"
        if get_cached_index(first_month_pid) is not None:
            log.info("TOWN SWEEP  %s %d (reconstructing from monthly sub-windows)", dir_label, y)
            monthly, month_trunc = _collect_sub_months(rl, direction, dir_label, y, stats, limit)
            sub_unique = _dedup(monthly)
            save_index(sub_pid, sub_unique, truncated=month_trunc)
            any_truncated |= month_trunc
            all_docs.extend(sub_unique)
            log.info("  → %d documents", len(sub_unique))
            continue

        if limit and stats["attempted"] >= limit:
            log.info("Reached --limit %d, stopping.", limit)
            break

        stats["attempted"] += 1
        log.info("TOWN SWEEP  %s %d–%d (sub-window)", dir_label, y, y)
        try:
            sub_docs, sub_trunc = name_search(
                rl, TOWN_SEARCH_NAME, TOWN_SEARCH_FIRST, False,
                str(y), str(y),
                sub_pid, "TOWN-SWEEP",
                direction=direction,
                max_pages=SWEEP_MAX_PAGES,
            )
        except RegistryThrottleError:
            raise
        except Exception as e:
            log.error("Town sweep sub-window error %s %d: %s", direction, y, e)
            stats["errors"] += 1
            continue

        if sub_trunc:
            log.info("  Subdividing year %d into monthly windows", y)
            monthly, month_trunc = _collect_sub_months(rl, direction, dir_label, y, stats, limit)
            sub_unique = _dedup(monthly)
            year_trunc = month_trunc
        else:
            sub_unique = _dedup(sub_docs)
            year_trunc = False

        save_index(sub_pid, sub_unique, truncated=year_trunc)
        any_truncated |= year_trunc
        all_docs.extend(sub_unique)
        log.info("  → %d documents", len(sub_unique))

    return all_docs, any_truncated


def process_town_sweep(rl: RateLimiter, limit: int) -> dict:
    windows = _date_windows()
    stats = {"windows_total": len(windows) * len(DIRECTIONS), "attempted": 0,
             "cache_hits": 0, "total_docs": 0, "errors": 0}

    for year_start, year_end in windows:
        for direction, dir_label in DIRECTIONS:
            pid = f"sweep-denn-{direction}-{year_start}"

            if get_cached_index(pid) is not None:
                stats["cache_hits"] += 1
                continue

            try:
                docs, window_truncated = _sweep_window(rl, direction, dir_label, year_start, year_end,
                                                       pid, stats, limit)
            except StopIteration:
                return stats
            except RegistryThrottleError:
                raise
            except Exception as e:
                log.error("Town sweep error %s %s-%s: %s", direction, year_start, year_end, e)
                stats["errors"] += 1
                continue

            unique = _dedup(docs)
            save_index(pid, unique, truncated=window_truncated)
            stats["total_docs"] += len(unique)
            log.info("  → %d documents", len(unique))

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--override-robots", action="store_true")
    parser.add_argument("--xrefs", action="store_true",
                        help="Run cross-reference expansion")
    parser.add_argument("--town-sweep", action="store_true",
                        help="Run Town of Dennis date-windowed sweep")
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after N network requests (0 = no limit)")
    args = parser.parse_args()

    if not args.xrefs and not args.town_sweep:
        parser.error("Specify at least one of --xrefs or --town-sweep")

    if not check_robots(override=args.override_robots):
        sys.exit(1)

    ensure_cache_dirs()
    log_path = setup_logging("sweep")
    log.info("Logging to %s", log_path)

    rl = RateLimiter()
    xref_stats: dict = {}
    town_stats: dict = {}

    try:
        if args.xrefs:
            targets = collect_xref_targets()
            log.info("Cross-ref targets: %d new book/page pairs", len(targets))
            xref_stats = process_xrefs(rl, targets, args.limit)

        if args.town_sweep:
            town_stats = process_town_sweep(rl, args.limit)

    except RegistryThrottleError as e:
        log.error("STOPPED: %s", e)
        sys.exit(1)
    finally:
        rl.close()

    lines = [
        "Registry Sweep Report",
        f"Generated: {datetime.now().isoformat()}",
    ]
    if xref_stats:
        lines += [
            "", "=== Cross-reference Expansion ===",
            f"Targets found:   {xref_stats.get('total', 0)}",
            f"Cache hits:      {xref_stats.get('cache_hits', 0)}",
            f"Attempted:       {xref_stats.get('attempted', 0)}",
            f"Succeeded:       {xref_stats.get('succeeded', 0)}",
            f"No result:       {xref_stats.get('no_result', 0)}",
            f"Errors:          {xref_stats.get('errors', 0)}",
        ]
    if town_stats:
        lines += [
            "", "=== Town of Dennis Sweep ===",
            f"Windows total:   {town_stats.get('windows_total', 0)}",
            f"Cache hits:      {town_stats.get('cache_hits', 0)}",
            f"Attempted:       {town_stats.get('attempted', 0)}",
            f"Documents found: {town_stats.get('total_docs', 0)}",
            f"Errors:          {town_stats.get('errors', 0)}",
        ]

    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text("\n".join(lines))
    log.info("Report: %s", REPORT_TXT)


if __name__ == "__main__":
    main()

"""
AgendaCenter pull orchestrator — scrape, ingest, and download.

Modes:
  --full   Scrape year-by-year from full_history_start_year to today, download all.
  --daily  (default) Scrape last daily_lookback_days days, download new.

Flags:
  --limit N       Cap downloads per run.
  --delay SEC     Seconds between PDF downloads (default 1.0).
  --start-date    Override scrape start date (YYYY-MM-DD).
  --end-date      Override scrape end date (YYYY-MM-DD).

Usage:
    python3 -m discovery.agenda_center.pull [--full | --daily]
"""

import argparse
import logging
import sys
from datetime import date

from discovery.agenda_center import scrape, ingest, download
from discovery.agenda_center.scrape import FULL_START
from discovery.config import get_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _scrape_full() -> int:
    """Scrape year-by-year from FULL_START to today. Returns total new docs ingested."""
    today = date.today()
    total_new = 0
    for year in range(FULL_START.year, today.year + 1):
        start = date(year, 1, 1)
        end = date(year, 12, 31) if year < today.year else today
        log.info("--- Scraping %d ---", year)
        csv_path = scrape.run(start_date=start, end_date=end)
        if csv_path is None:
            log.info("  No assets found for %d", year)
            continue
        new = ingest.run(csv_path=csv_path)
        log.info("  Ingested %d new doc(s) for %d", new, year)
        total_new += new
    return total_new


def main() -> None:
    if not get_config().enabled("agenda_center"):
        log.info("agenda_center source is disabled in sources.yaml — exiting")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Pull AgendaCenter documents for Dennis, MA")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full",  action="store_true", help="Scrape year-by-year from start to today")
    mode.add_argument("--daily", action="store_true", help="Scrape last N days (default)")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap downloads per run")
    parser.add_argument("--delay", type=float, default=1.0, metavar="SEC",
                        help="Seconds between PDF downloads (default 1.0)")
    parser.add_argument("--start-date", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--end-date",   type=date.fromisoformat, default=None, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    try:
        if args.full:
            log.info("=== Full scrape (year-by-year %d→today) ===", FULL_START.year)
            total_new = _scrape_full()
            log.info("Full scrape complete: %d new document(s) total", total_new)
        else:
            log.info("=== Stage 1: Scrape ===")
            csv_path = scrape.run(start_date=args.start_date, end_date=args.end_date)
            if csv_path is None:
                log.info("No new assets found — nothing to do.")
                sys.exit(0)
            log.info("=== Stage 2: Ingest ===")
            total_new = ingest.run(csv_path=csv_path)
            log.info("Ingested %d new document(s)", total_new)

        log.info("=== Stage 3: Download ===")
        dl_ok, dl_err = download.run(limit=args.limit, delay=args.delay)
        log.info("Download complete: %d ok, %d errors", dl_ok, dl_err)
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()

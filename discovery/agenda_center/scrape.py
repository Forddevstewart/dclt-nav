"""
Scrape meeting agendas and minutes from Dennis, MA CivicPlus AgendaCenter.

Produces a CSV of asset metadata in the civic_scraper format.
Returns the path to the written CSV, or None if no assets found.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

from civic_scraper.platforms import CivicPlusSite
from civic_scraper.base.cache import Cache

from discovery.config import get_config, LOCAL_OUTPUT_DIR

METADATA_DIR = LOCAL_OUTPUT_DIR / "agendacenter_metadata"

log = logging.getLogger(__name__)


def _cfg():
    return get_config().source("agenda_center")


def run(start_date: date | None = None, end_date: date | None = None) -> Path | None:
    """
    Scrape the Dennis CivicPlus site for the given date range.
    Defaults to the last daily_lookback_days days.
    Returns the path to the written CSV, or None if no assets found.
    """
    cfg = _cfg()
    site_url = cfg["base_url"]
    lookback = int(cfg.get("daily_lookback_days", 14))

    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=lookback)

    log.info("Scraping CivicPlus %s → %s", start_date, end_date)

    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    site = CivicPlusSite(site_url, place_name="Dennis")
    assets = site.scrape(
        start_date=str(start_date),
        end_date=str(end_date),
        cache=True,
        download=False,
    )

    if not assets:
        log.info("No assets found for %s → %s", start_date, end_date)
        return None

    log.info("Found %d asset(s)", len(assets))
    cache = Cache(str(METADATA_DIR))
    csv_path = Path(assets.to_csv(cache.metadata_files_path))
    log.info("Metadata written to %s", csv_path)
    return csv_path


FULL_START = date(
    get_config().source("agenda_center").get("full_history_start_year", 2010), 1, 1
)

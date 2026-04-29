"""
Scrape meeting agendas and minutes from Dennis, MA CivicPlus AgendaCenter.

Produces a CSV of asset metadata in the civic_scraper format.
Returns the path to the written CSV, or None if no assets found.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

import requests
import civic_scraper
from civic_scraper.platforms import CivicPlusSite
from civic_scraper.base.asset import Asset, AssetCollection
from civic_scraper.base.cache import Cache

from discovery.config import get_config, LOCAL_OUTPUT_DIR

METADATA_DIR = LOCAL_OUTPUT_DIR / "agendacenter_metadata"

log = logging.getLogger(__name__)


class _RobustCivicPlusSite(CivicPlusSite):
    """CivicPlusSite that tolerates HEAD-request failures (e.g. server SSL resets)."""

    def _build_asset_collection(self, metadata):
        assets = AssetCollection()
        for row in metadata:
            url = self._mk_url(self.url, row["url_path"])
            asset_args = {
                "state_or_province": self.state_or_province,
                "place": self.place,
                "place_name": self.place_name,
                "committee_name": row["committee_name"],
                "meeting_id": self._mk_mtg_id(self.subdomain, row["meeting_id"]),
                "meeting_date": row["meeting_date"],
                "meeting_time": row["meeting_time"],
                "asset_name": row["meeting_title"],
                "asset_type": row["asset_type"],
                "scraped_by": f"civic-scraper_{civic_scraper.__version__}",
                "url": url,
            }
            try:
                headers = requests.head(url, allow_redirects=True, timeout=10).headers
                asset_args["content_type"] = headers.get("content-type", "application/octet-stream")
                asset_args["content_length"] = headers.get("content-length", -1)
            except Exception:
                log.warning("HEAD request failed for %s — skipping header fetch", url)
                asset_args["content_type"] = "application/octet-stream"
                asset_args["content_length"] = -1
            assets.append(Asset(**asset_args))
        return assets


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

    site = _RobustCivicPlusSite(site_url, place_name="Dennis")
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

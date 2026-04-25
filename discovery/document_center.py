"""
Discovery — Download all Dennis MA DocumentCenter PDFs.

Covers all categories in sources.yaml under document_center.categories:
  warrants              Annual warrant PDFs (keyword-filtered to warrant/petition docs)
  results               Town meeting voting result PDFs
  election_results      Election result PDFs
  bylaws_regulations    Bylaws and regulations PDFs
  comprehensive_plan    Comprehensive plan PDFs
  budget                Budget PDFs
  open_space_plan       Open space and recreation plan PDFs
  town_administrator_reports  Town administrator report PDFs

Idempotent: skips files already downloaded. Re-run any time.

Usage:
    python3 -m discovery.document_center
"""

import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from discovery.config import get_config

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "DennisCivicResearch/1.0 (civic research; contact: ford.stewart@pm.me)"}

# Warrant filtering — only download documents matching these keywords on the /676/ page
_WARRANT_KEYWORDS = {"warrant", "petition"}
_WARRANT_SKIP = {
    "budget", "report", "audit", "fee book", "administrator", "schedule",
    "goals", "liaison", "exemption", "inventory", "presentation",
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _session(base: str, delay: float) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(base, timeout=15)
    time.sleep(delay)
    return s


def _get(session: requests.Session, url: str, delay: float) -> requests.Response:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    time.sleep(delay)
    return resp


def _download(session: requests.Session, url: str, dest: Path, delay: float) -> bool:
    if dest.exists():
        log.info("  SKIP  %s", dest.name)
        return True
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        time.sleep(delay)
        if len(resp.content) < 500:
            log.warning("  TINY  %d bytes — %s", len(resp.content), url)
            return False
        dest.write_bytes(resp.content)
        log.info("  OK    %s  (%.0f KB)", dest.name, len(resp.content) / 1024)
        return True
    except Exception as e:
        log.error("  FAIL  %s: %s", url, e)
        if dest.exists():
            dest.unlink()
        return False


# ── Filename helpers ──────────────────────────────────────────────────────────

def _filename_from_href(href: str, link_text: str) -> str:
    """Build a clean filename from the DocumentCenter URL slug and link text."""
    m = re.search(r'/DocumentCenter/View/(\d+)/([^"?\s]+)', href)
    if m:
        doc_id, slug = m.group(1), m.group(2)
        slug = re.sub(r'-PDF$', '', slug, flags=re.IGNORECASE).lower()
        return f"{doc_id}_{slug}.pdf"
    slug = re.sub(r'[^\w\s-]', '', link_text).strip()
    slug = re.sub(r'\s+', '-', slug).lower()
    return f"{slug}.pdf"


def _slugify_result(text: str) -> str:
    """Turn '2023 Annual Town Meeting (PDF)' → '2023_annual_town_meeting.pdf'."""
    text = re.sub(r'\s*\(PDF\)\s*', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'\s+', '_', text).lower()
    return text + ".pdf"


# ── Category-specific scrapers ────────────────────────────────────────────────

def _scrape_generic(session: requests.Session, base: str, path: str, delay: float) -> list[dict]:
    """Return [{url, filename}] for all DocumentCenter links on a standard library page."""
    resp = _get(session, base + path, delay)
    soup = BeautifulSoup(resp.text, "lxml")

    results = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "DocumentCenter/View" not in href:
            continue
        full_url = base + href if href.startswith("/") else href
        if full_url in seen:
            continue
        seen.add(full_url)
        text = a.get_text(strip=True)
        filename = _filename_from_href(href, text or "document")
        results.append({"url": full_url, "filename": filename})
    return results


def _scrape_warrants(session: requests.Session, base: str, path: str, delay: float) -> list[dict]:
    """
    Parse the annual warrants page for warrant/petition PDFs.
    Captures year from section headings; skips budget/report/admin documents.
    Returns [{url, filename}].
    """
    resp = _get(session, base + path, delay)
    soup = BeautifulSoup(resp.text, "lxml")

    results = []
    current_year = "unknown"
    seen: set[str] = set()

    for el in soup.find_all(["h2", "h3", "h4", "strong", "b", "a"]):
        if el.name in ("h2", "h3", "h4", "strong", "b"):
            text = el.get_text(strip=True)
            m = re.search(r'(20\d{2}|19\d{2})', text)
            if m:
                current_year = m.group(1)
        elif el.name == "a" and el.get("href", ""):
            href = el["href"]
            if "DocumentCenter/View" not in href:
                continue
            link_text = el.get_text(strip=True)
            t = link_text.lower()
            if any(k in t for k in _WARRANT_SKIP):
                continue
            if not any(k in t for k in _WARRANT_KEYWORDS):
                continue
            url = base + href if href.startswith("/") else href
            if url in seen:
                continue
            seen.add(url)
            kind = "special" if "special" in t else "annual"
            qualifier = ""
            for month in ["july", "october", "february", "november"]:
                if month in t:
                    qualifier = f"_{month}"
            filename = f"{current_year}_{kind}{qualifier}_warrant.pdf"
            results.append({"url": url, "filename": filename})
    return results


def _scrape_results(session: requests.Session, base: str, path: str, delay: float) -> list[dict]:
    """Parse the meeting results page for voting result PDFs."""
    resp = _get(session, base + path, delay)
    soup = BeautifulSoup(resp.text, "lxml")

    results = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "DocumentCenter/View" not in href:
            continue
        text = a.get_text(strip=True)
        if not text or len(text) < 5:
            continue
        url = base + href if href.startswith("/") else href
        if url in seen:
            continue
        seen.add(url)
        results.append({"url": url, "filename": _slugify_result(text)})
    return results


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run() -> tuple[int, int]:
    """Download all DocumentCenter categories. Returns (ok, failed)."""
    cfg = get_config()
    if not cfg.enabled("document_center"):
        log.info("document_center source is disabled in sources.yaml — skipping")
        return 0, 0

    src = cfg.source("document_center")
    base = src["base_url"]
    delay = float(src.get("rate_limit_seconds", 2.0))
    out_root = cfg.output_dir("document_center")
    categories = src["categories"]

    session = _session(base, delay)
    ok = failed = 0

    for cat_id, cat in categories.items():
        path = cat["path"]
        subdir = cat["subdir"]
        dest_dir = out_root / subdir
        dest_dir.mkdir(parents=True, exist_ok=True)

        log.info("=== %s → %s ===", path, subdir)

        if cat_id == "warrants":
            docs = _scrape_warrants(session, base, path, delay)
        elif cat_id == "results":
            docs = _scrape_results(session, base, path, delay)
        else:
            docs = _scrape_generic(session, base, path, delay)

        log.info("  Found %d documents", len(docs))
        for doc in docs:
            dest = dest_dir / doc["filename"]
            if _download(session, doc["url"], dest, delay):
                ok += 1
            else:
                failed += 1

    log.info("DocumentCenter complete: %d ok, %d failed", ok, failed)
    return ok, failed


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    run()


if __name__ == "__main__":
    main()

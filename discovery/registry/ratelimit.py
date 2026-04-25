"""Shared rate limiting and HTTP session for the Barnstable Registry pipeline.

robots.txt at search.barnstabledeeds.org says Disallow: /
The override_robots flag in sources.yaml must be set to true to proceed,
or pass --override-robots on the CLI to override for a single session.
"""

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from discovery.config import get_config, LOCAL_OUTPUT_DIR

REGISTRY_BASE = "https://search.barnstabledeeds.org"
USER_AGENT = "DennisCivicResearch/1.0 (civic research; contact: ford.stewart@pm.me)"

ACCESS_LOG = LOCAL_OUTPUT_DIR / "registry_access.log"

log = logging.getLogger(__name__)


def _rate_limit_cfg() -> dict:
    return get_config().source("registry").get("rate_limit", {})


def check_robots(override: bool = False) -> bool:
    """Return True if allowed to crawl; False if robots.txt forbids it."""
    from urllib.robotparser import RobotFileParser

    rp = RobotFileParser()
    rp.set_url(f"{REGISTRY_BASE}/robots.txt")
    try:
        rp.read()
    except Exception as e:
        log.warning("Could not fetch robots.txt: %s — proceeding cautiously", e)
        return True

    # Also check the config-level default
    config_override = get_config().registry_override_robots()
    effective_override = override or config_override

    allowed = rp.can_fetch(USER_AGENT, REGISTRY_BASE + "/")
    if not allowed:
        msg = (
            f"\n{'='*60}\n"
            "robots.txt at search.barnstabledeeds.org says Disallow: /\n"
            "This is a public records system. The restriction likely\n"
            "targets commercial scrapers, not civic research.\n\n"
            "To proceed, set override_robots: true in sources.yaml\n"
            "or re-run with --override-robots\n"
            "By doing so you acknowledge this disallow and accept\n"
            "responsibility for this access.\n"
            f"{'='*60}\n"
        )
        if effective_override:
            log.warning("robots.txt Disallow overridden")
            print(msg.replace(
                "set override_robots: true in sources.yaml\nor re-run with --override-robots",
                "robots.txt override active",
            ), file=sys.stderr)
            return True
        print(msg, file=sys.stderr)
        return False
    return True


class RateLimiter:
    def __init__(self):
        cfg = _rate_limit_cfg()
        self._min_delay: float = cfg.get("min_delay_seconds", 3.0)
        self._adaptive_delay: float = cfg.get("adaptive_delay_seconds", 10.0)
        self._batch_pause: float = cfg.get("batch_pause_seconds", 30.0)
        self._batch_size: int = cfg.get("batch_size", 20)
        self._slow_threshold: float = cfg.get("slow_threshold_seconds", 10.0)
        self._max_retries: int = cfg.get("max_retries", 2)

        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT
        self._last_request_time: float = 0.0
        self._request_count: int = 0
        self._adaptive_active: int = 0

        ACCESS_LOG.parent.mkdir(parents=True, exist_ok=True)
        self._access_log = ACCESS_LOG.open("a")

    def _log_access(self, url: str, status: int, elapsed: float) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self._access_log.write(f"{ts}\t{status}\t{elapsed:.2f}s\t{url}\n")
        self._access_log.flush()
        log.debug("ACCESS %s %s (%.2fs)", status, url, elapsed)

    def _enforce_delay(self) -> None:
        delay = self._adaptive_delay if self._adaptive_active > 0 else self._min_delay
        if self._adaptive_active > 0:
            self._adaptive_active -= 1

        elapsed_since_last = time.monotonic() - self._last_request_time
        remaining = delay - elapsed_since_last
        if remaining > 0:
            time.sleep(remaining)

        if self._request_count > 0 and self._request_count % self._batch_size == 0:
            log.info("Batch pause %ds after %d requests", self._batch_pause, self._request_count)
            time.sleep(self._batch_pause)

    def get(self, url: str, params: dict | None = None, **kwargs) -> requests.Response:
        self._enforce_delay()

        for attempt in range(self._max_retries + 1):
            try:
                t0 = time.monotonic()
                self._last_request_time = t0
                self._request_count += 1

                resp = self._session.get(url, params=params, timeout=30, **kwargs)
                elapsed = time.monotonic() - t0

                self._log_access(resp.url, resp.status_code, elapsed)

                if resp.status_code in (429, 503):
                    log.error("HTTP %s from Registry — stopping. URL: %s", resp.status_code, resp.url)
                    raise RegistryThrottleError(
                        f"Registry returned {resp.status_code}. "
                        "Do not retry automatically. Investigate before continuing."
                    )

                if elapsed > self._slow_threshold:
                    log.info(
                        "Slow response (%.1fs) — activating adaptive delay for next 10 requests",
                        elapsed,
                    )
                    self._adaptive_active = 10

                return resp

            except RegistryThrottleError:
                raise
            except requests.RequestException as e:
                if attempt < self._max_retries:
                    log.warning("Request failed (attempt %d): %s — retrying", attempt + 1, e)
                    time.sleep(self._min_delay)
                else:
                    log.error("Request failed after %d attempts: %s", self._max_retries + 1, e)
                    raise

        raise RuntimeError("Unreachable")

    def close(self) -> None:
        self._access_log.close()
        self._session.close()

    @property
    def request_count(self) -> int:
        return self._request_count


class RegistryThrottleError(Exception):
    """Raised on HTTP 429 or 503 from the Registry. Do not catch automatically."""

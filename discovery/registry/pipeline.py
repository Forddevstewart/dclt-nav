"""Registry refresh pipeline — enumeration → build → download.

Run monthly to pick up newly recorded documents.

Usage:
    python3 -m discovery.registry.pipeline                    # enumerate + build + manifest
    python3 -m discovery.registry.pipeline --confirm          # also download PDFs
    python3 -m discovery.registry.pipeline --limit 200        # cap network requests per step
    python3 -m discovery.registry.pipeline --override-robots
"""

import argparse
import csv
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from discovery.config import get_config, LOCAL_OUTPUT_DIR
from discovery.registry.cache import ensure_cache_dirs, setup_logging
from discovery.registry.download import build_download_manifest, download_queue
from discovery.registry.enumerate import process_tier1, process_tier2
from discovery.registry.ratelimit import RateLimiter, RegistryThrottleError, check_robots
from discovery.registry.sweep import collect_xref_targets, process_town_sweep, process_xrefs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

REPORT_TXT = LOCAL_OUTPUT_DIR / "registry_pipeline_report.txt"


def _queue_csv() -> Path:
    return get_config().output_dir("registry") / "queue" / "target_queue.csv"


def _banner(label: str) -> None:
    log.info("")
    log.info("━━━  %s  ━━━", label)


def _build() -> None:
    _banner("Build — rebuild database from registry index")
    result = subprocess.run([sys.executable, "-m", "processing.build"])
    if result.returncode != 0:
        log.error("Build failed (exit %d) — aborting.", result.returncode)
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--override-robots", action="store_true")
    parser.add_argument("--confirm", action="store_true",
                        help="Download PDFs after enumeration (default: manifest only)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap network requests per enumeration step (0 = no limit)")
    args = parser.parse_args()

    if not check_robots(override=args.override_robots):
        sys.exit(1)

    ensure_cache_dirs()
    log_path = setup_logging("pipeline")
    log.info("Logging to %s", log_path)

    queue_csv = _queue_csv()
    if not queue_csv.exists():
        log.error("Queue not found: %s — run discovery.registry.queue first.", queue_csv)
        sys.exit(1)

    with queue_csv.open(newline="", encoding="utf-8") as f:
        queue = list(csv.DictReader(f))
    log.info("Loaded %d parcels from queue", len(queue))

    started = datetime.now()
    log.info("Registry refresh started %s", started.strftime("%Y-%m-%d %H:%M"))

    all_stats: dict = {}
    rl = RateLimiter()

    try:
        _banner("Step 1 — Book/page lookups (Tier 1)")
        all_stats["tier1"] = process_tier1(rl, queue, start_after="", limit=args.limit)

        _banner("Step 2 — Name searches (Tier 2)")
        all_stats["tier2"] = process_tier2(rl, queue, limit=args.limit)

        _banner("Step 3 — Cross-reference expansion")
        xref_targets = collect_xref_targets()
        log.info("Cross-ref targets: %d new book/page pairs", len(xref_targets))
        all_stats["xrefs"] = process_xrefs(rl, xref_targets, limit=args.limit)

        _banner("Step 4 — Town of Dennis date-windowed sweep")
        all_stats["town"] = process_town_sweep(rl, limit=args.limit)

    except RegistryThrottleError as e:
        log.error("STOPPED by throttle: %s", e)
        sys.exit(1)
    finally:
        rl.close()

    _build()

    _banner("Step 5 — Download manifest")
    dl_queue, manifest_stats = build_download_manifest()
    print(f"\n{'='*60}")
    print(f"  Total approved instruments:  {manifest_stats.get('total_approved', 0)}")
    print(f"  Already cached (skip):       {manifest_stats.get('already_cached', 0)}")
    print(f"  Queued for download:         {manifest_stats.get('to_download', 0)}")
    print()
    for t, c in sorted(manifest_stats.get("type_counts", {}).items(), key=lambda x: -x[1]):
        print(f"    {c:4d}  {t}")
    print(f"{'='*60}\n")

    dl_stats: dict = {}
    if dl_queue and args.confirm:
        _banner("Step 6 — Downloading PDFs")
        rl2 = RateLimiter()
        try:
            dl_stats = download_queue(rl2, dl_queue, args.limit or len(dl_queue))
        finally:
            rl2.close()
        log.info("Downloaded: %d  Failed: %d  Size: %.1f MB",
                 dl_stats.get("succeeded", 0), dl_stats.get("failed", 0),
                 dl_stats.get("total_bytes", 0) / 1024 / 1024)
    elif dl_queue:
        log.info("%d documents available — pass --confirm to download PDFs.",
                 manifest_stats.get("to_download", 0))

    elapsed = int((datetime.now() - started).total_seconds()) // 60
    lines = [
        "Registry Pipeline Report",
        f"Generated: {datetime.now().isoformat()}",
        f"Elapsed:   {elapsed} min",
        "",
        "=== Step 1: Tier 1 (Book/Page) ===",
        f"  Cache hits:  {all_stats.get('tier1', {}).get('cache_hits', 0)}",
        f"  Lookups:     {all_stats.get('tier1', {}).get('attempted', 0)}",
        f"  Succeeded:   {all_stats.get('tier1', {}).get('succeeded', 0)}",
        f"  No result:   {all_stats.get('tier1', {}).get('no_result', 0)}",
        "",
        "=== Step 2: Tier 2 (Name Search) ===",
        f"  Cache hits:  {all_stats.get('tier2', {}).get('cache_hits', 0)}",
        f"  Searches:    {all_stats.get('tier2', {}).get('attempted', 0)}",
        f"  Succeeded:   {all_stats.get('tier2', {}).get('succeeded', 0)}",
        "",
        "=== Step 3: Cross-references ===",
        f"  Targets:     {all_stats.get('xrefs', {}).get('total', 0)}",
        f"  Cache hits:  {all_stats.get('xrefs', {}).get('cache_hits', 0)}",
        f"  Succeeded:   {all_stats.get('xrefs', {}).get('succeeded', 0)}",
        "",
        "=== Step 4: Town Sweep ===",
        f"  Cache hits:  {all_stats.get('town', {}).get('cache_hits', 0)}",
        f"  Attempted:   {all_stats.get('town', {}).get('attempted', 0)}",
        f"  Documents:   {all_stats.get('town', {}).get('total_docs', 0)}",
        "",
        "=== Download Manifest ===",
        f"  Approved:    {manifest_stats.get('total_approved', 0)}",
        f"  Cached:      {manifest_stats.get('already_cached', 0)}",
        f"  To download: {manifest_stats.get('to_download', 0)}",
    ]
    if dl_stats:
        lines += [
            "",
            "=== Downloads ===",
            f"  Succeeded:   {dl_stats.get('succeeded', 0)}",
            f"  Failed:      {dl_stats.get('failed', 0)}",
            f"  Size:        {dl_stats.get('total_bytes', 0) / 1024 / 1024:.1f} MB",
        ]

    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text("\n".join(lines))
    log.info("Report: %s", REPORT_TXT)
    log.info("Done (%d min).", elapsed)


if __name__ == "__main__":
    main()

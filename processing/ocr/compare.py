"""
OCR comparison — evaluate Tesseract vs VLM impact on a document.

Reads a scan.json produced by ocr_pipeline.py + vlm_repass.py and shows,
for each page, how keyword scores changed when VLM was added.

Usage:
    python3 -m processing.ocr.compare path/to/scan.json
    python3 -m processing.ocr.compare path/to/scan.json --threshold 0.4
    python3 -m processing.ocr.compare path/to/scan.json --text
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from processing.ocr.ocr_pipeline import COMPOSITE_WEIGHTS


def composite_without_vlm(components: dict[str, Optional[float]]) -> float:
    available = {k: v for k, v in components.items()
                 if k != "vlm_classifier" and v is not None}
    if not available:
        return 0.0
    total_weight = sum(COMPOSITE_WEIGHTS[k] for k in available)
    return sum(COMPOSITE_WEIGHTS[k] * v for k, v in available.items()) / total_weight


def fmt(v: Optional[float]) -> str:
    if v is None:
        return "  — "
    return f"{v:.3f}"


def threshold_marker(before: float, after: float, threshold: float) -> str:
    crossed_up   = before < threshold <= after
    crossed_down = after  < threshold <= before
    if crossed_up:
        return " ▲"
    if crossed_down:
        return " ▼"
    return ""


def show_page(page: dict, threshold: float, show_text: bool) -> None:
    page_num     = page.get("page_number", "?")
    engines      = page.get("engines_used", ["tesseract"])
    kw_scores    = page.get("keyword_scores", {})
    has_vlm      = "vlm" in engines

    print(f"\n  Page {page_num}  [{', '.join(engines)}]")

    if not kw_scores:
        print("    (no keyword scores)")
        return

    # Header
    print(f"    {'keyword':<40}  {'tess':>6}  {'full':>6}  {'delta':>6}  "
          f"{'exact':>5}  {'fuzzy':>5}  {'ctx':>5}  {'vlm':>6}")
    print("    " + "-" * 95)

    any_change = False
    for name, scores in sorted(kw_scores.items()):
        comps       = scores.get("components", {})
        full        = scores.get("composite", 0.0)
        tess        = composite_without_vlm(comps)
        delta       = full - tess
        vlm_val     = comps.get("vlm_classifier")
        marker      = threshold_marker(tess, full, threshold) if has_vlm else ""

        if abs(delta) > 0.001 or full >= threshold * 0.5:
            any_change = True

        print(f"    {name:<40}  {fmt(tess):>6}  {fmt(full):>6}  "
              f"{delta:>+.3f}  "
              f"{fmt(comps.get('exact_match')):>5}  "
              f"{fmt(comps.get('fuzzy_match')):>5}  "
              f"{fmt(comps.get('context')):>5}  "
              f"{fmt(vlm_val):>6}"
              f"{marker}")

    if show_text and has_vlm:
        text = page.get("text", "")
        lines = text.splitlines()
        # Lines after the halfway point are likely VLM additions
        # (union_texts appends unique VLM lines after Tesseract lines)
        print(f"\n    ── text ({len(lines)} lines) ──")
        for line in lines:
            print(f"    {line}")


def compare(path: Path, threshold: float, show_text: bool) -> None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        sys.exit(1)

    if "error" in data:
        print(f"Document has error: {data['error']}", file=sys.stderr)
        sys.exit(1)

    pages      = data.get("pages", [])
    processed  = data.get("processed_at", "unknown")
    version    = data.get("pipeline_version", "?")
    n_pages    = data.get("page_count", len(pages))
    enriched   = [p for p in pages if "vlm" in p.get("engines_used", [])]

    print(f"\n{'='*70}")
    print(f"  {path}")
    print(f"  {n_pages} page(s)  |  pipeline v{version}  |  processed {processed}")
    print(f"  VLM-enriched pages: {len(enriched)} / {n_pages}  |  threshold: {threshold}")
    print(f"{'='*70}")

    # Per-page detail
    for page in pages:
        show_page(page, threshold, show_text)

    # Document-level summary: max composite per keyword, tess vs full
    print(f"\n  {'─'*68}")
    print(f"  {'SUMMARY — max composite per keyword across all pages':}")
    print(f"  {'keyword':<40}  {'tess':>6}  {'full':>6}  {'delta':>6}")
    print(f"  {'─'*68}")

    all_keywords: set[str] = set()
    for page in pages:
        all_keywords.update(page.get("keyword_scores", {}).keys())

    for name in sorted(all_keywords):
        tess_max = 0.0
        full_max = 0.0
        for page in pages:
            scores = page.get("keyword_scores", {}).get(name, {})
            comps  = scores.get("components", {})
            full   = scores.get("composite", 0.0)
            tess   = composite_without_vlm(comps)
            tess_max = max(tess_max, tess)
            full_max = max(full_max, full)

        delta  = full_max - tess_max
        marker = threshold_marker(tess_max, full_max, threshold)
        print(f"  {name:<40}  {fmt(tess_max):>6}  {fmt(full_max):>6}  "
              f"{delta:>+.3f}{marker}")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("scan_json", type=Path, nargs="+",
                        help="Path(s) to scan.json file(s)")
    parser.add_argument("--threshold", type=float, default=0.4,
                        help="Score threshold to flag crossings with ▲/▼ (default 0.4)")
    parser.add_argument("--text", action="store_true",
                        help="Also print the full merged text for VLM-enriched pages")
    args = parser.parse_args()

    for path in args.scan_json:
        compare(path, args.threshold, args.text)


if __name__ == "__main__":
    main()

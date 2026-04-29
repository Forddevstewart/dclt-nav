"""
VLM re-pass — targeted VLM enrichment of candidate OCR pages.

Walks one or more input roots looking for .json files produced by
ocr_pipeline.py. For each document that passes the document-type filter,
checks each page for the candidate condition:

    composite >= min_composite AND exact_match == 0.0 (for any keyword)

For candidate pages:
  1. Rasterizes the specific page from the original PDF (300 DPI)
  2. Runs VLM transcription — merges any new text into the page and
     re-scores exact/fuzzy/context components with the improved text
  3. Runs VLM classifier per keyword — asks the model directly whether
     the concept is present and adds vlm_classifier to each keyword's
     components
  4. Recomputes composite for all keywords
  5. Writes the updated JSON atomically back alongside the PDF

Idempotent: pages that already have a non-null vlm_classifier are skipped
unless --force is set.

Usage:
    python3 -m processing.ocr.vlm_repass \\
        --input-roots /Volumes/DigitalTwin/CivicTwin/registry/documents \\
                      /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter \\
                      /Volumes/DigitalTwin/CivicTwin/ma-dennis/warrants/pdfs
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Optional

from pdf2image import convert_from_path
from PIL import Image

from processing.ocr.ocr_pipeline import (
    COMPOSITE_WEIGHTS,
    KeywordConfig,
    compute_composite,
    load_keywords,
    run_vlm_transcription,
    score_context,
    score_exact_match,
    score_fuzzy_match,
    score_vlm_classifier,
    union_texts,
)

log = logging.getLogger(__name__)

# Committees whose *minutes* merit VLM analysis
COMMITTEE_KEYWORDS = [
    "conservation",
    "select_board",
    "planning",
    "community_preservation",
    "waterways",
    "historical",
    "affordable_housing",
    "open_space",
    "zoning",
]


# ── Document-type filter ──────────────────────────────────────────────────────

def is_candidate_document(json_path: Path, committees: list[str]) -> bool:
    """Return True if this document type merits VLM analysis."""
    p = str(json_path).lower()

    if "registry/documents" in p:
        return True

    if "warrants/pdfs" in p:
        return True

    if "documentcenter" in p:
        return True

    if "agendacenter" in p:
        # Only minutes, not agendas
        if not json_path.stem.lower().startswith("minutes_"):
            return False
        committee = json_path.parent.name.lower()
        return any(kw in committee for kw in committees)

    return False


# ── Page-level candidate check ────────────────────────────────────────────────

def is_candidate_page(page: dict, min_composite: float) -> bool:
    """Page is a candidate if any keyword has composite >= threshold with exact_match == 0."""
    for kw_scores in page.get("keyword_scores", {}).values():
        composite = kw_scores.get("composite", 0.0)
        exact = kw_scores.get("components", {}).get("exact_match", 0.0)
        if composite >= min_composite and exact == 0.0:
            return True
    return False


def already_scored(page: dict) -> bool:
    """Return True if every keyword already has a non-null vlm_classifier score."""
    scores = page.get("keyword_scores", {})
    if not scores:
        return False
    return all(
        kw.get("components", {}).get("vlm_classifier") is not None
        for kw in scores.values()
    )


# ── Page enrichment ───────────────────────────────────────────────────────────

def enrich_page(
    page: dict,
    img: Image.Image,
    keywords: dict[str, KeywordConfig],
    vlm_model: str,
    ollama_url: str,
) -> dict:
    """
    Run VLM transcription + classification on one page.
    Returns an updated copy of the page dict.
    """
    page = dict(page)

    # Step 1: VLM transcription — may recover text OCR missed
    vlm_text = run_vlm_transcription(img, vlm_model, ollama_url)
    if vlm_text:
        page["vlm_transcription"] = vlm_text  # preserved for compare.py
        merged = union_texts(page.get("text", ""), vlm_text)
        if merged != page.get("text", ""):
            page["text"] = merged
            if "vlm" not in page.get("engines_used", []):
                page["engines_used"] = page.get("engines_used", []) + ["vlm"]
            log.debug("  VLM transcription added %d new chars",
                      len(merged) - len(page.get("text", "")))

    text = page["text"]

    # Step 2: re-score and add vlm_classifier per keyword
    updated_scores: dict[str, dict] = {}
    for name, kw in keywords.items():
        existing = page.get("keyword_scores", {}).get(name, {})
        exact  = score_exact_match(text, kw)
        # Skip VLM classifier when Tesseract already has a confirmed exact match —
        # a VLM false-negative would otherwise dilute a strong signal.
        vlm_score = None if exact == 1.0 else score_vlm_classifier(img, name, vlm_model, ollama_url)
        components: dict[str, Optional[float]] = {
            "exact_match": exact,
            "fuzzy_match": score_fuzzy_match(text, kw),
            "context":     score_context(text, kw),
            "vlm_classifier": vlm_score,
        }
        composite = compute_composite(components)
        updated_scores[name] = {
            "composite": round(composite, 4),
            "components": {
                k: (round(v, 4) if v is not None else None)
                for k, v in components.items()
            },
        }

    page["keyword_scores"] = updated_scores
    return page


# ── Per-document processing ───────────────────────────────────────────────────

def process_document(
    json_path: Path,
    keywords: dict[str, KeywordConfig],
    min_composite: float,
    force: bool,
    vlm_model: str,
    ollama_url: str,
) -> tuple[str, int, int, float]:
    """
    Enrich candidate pages in one document.
    Returns (status, pages_enriched, pages_skipped, elapsed).
    """
    t0 = time.monotonic()
    pdf_path = json_path.with_suffix(".pdf")
    if not pdf_path.exists():
        # Try uppercase extension
        pdf_path = json_path.with_suffix(".PDF")
    if not pdf_path.exists():
        log.warning("PDF not found for %s — skipping", json_path)
        return "no_pdf", 0, 0, time.monotonic() - t0

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Could not read %s: %s", json_path, e)
        return "unreadable", 0, 0, time.monotonic() - t0

    if "error" in data:
        return "skipped_error", 0, 0, time.monotonic() - t0

    pages: list[dict] = data.get("pages", [])
    enriched = 0
    skipped = 0

    for i, page in enumerate(pages):
        if not force and already_scored(page):
            skipped += 1
            continue
        if not is_candidate_page(page, min_composite):
            skipped += 1
            continue

        page_num = page.get("page_number", i + 1)
        log.debug("  page %d is a candidate", page_num)

        try:
            images = convert_from_path(
                str(pdf_path), dpi=300,
                first_page=page_num, last_page=page_num,
            )
            if not images:
                log.warning("  Could not rasterize page %d of %s", page_num, pdf_path.name)
                skipped += 1
                continue
            img = images[0]
        except Exception as e:
            log.warning("  Rasterize error page %d: %s", page_num, e)
            skipped += 1
            continue

        pages[i] = enrich_page(page, img, keywords, vlm_model, ollama_url)
        enriched += 1
        log.info("  page %d enriched", page_num)

    if enriched == 0:
        return "no_candidates", 0, skipped, time.monotonic() - t0

    # Atomic write
    data["pages"] = pages
    tmp = json_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.rename(json_path)

    return "enriched", enriched, skipped, time.monotonic() - t0


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="VLM re-pass — targeted enrichment of candidate OCR pages"
    )
    parser.add_argument("--input-roots", type=Path, nargs="+", required=True,
                        metavar="PATH", help="Roots to walk for OCR JSON files")
    parser.add_argument("--keywords", type=Path,
                        default=Path(__file__).parent / "keywords.yaml")
    parser.add_argument("--min-composite", type=float, default=0.15, metavar="FLOAT",
                        help="Min composite score to be a candidate (default 0.15)")
    parser.add_argument("--committees", type=str,
                        default=",".join(COMMITTEE_KEYWORDS),
                        help="Comma-separated committee name substrings to include")
    parser.add_argument("--vlm-model", default="qwen2.5vl:7b")
    parser.add_argument("--vlm-url", default="http://localhost:11434")
    parser.add_argument("--force", action="store_true",
                        help="Re-run VLM even on pages already scored")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report candidate counts without running VLM")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    keywords = load_keywords(args.keywords)
    committees = [c.strip().lower() for c in args.committees.split(",")]
    log.info("Loaded %d keyword groups", len(keywords))
    log.info("Committee filter: %s", committees)

    # Collect all candidate JSON files
    json_files: list[Path] = []
    for root in args.input_roots:
        json_files.extend(sorted(root.rglob("*.json")))

    candidates = [
        p for p in json_files
        if is_candidate_document(p, committees)
        and not p.suffix == ".tmp"
        and (p.with_suffix(".pdf").exists() or p.with_suffix(".PDF").exists())
    ]
    log.info("Found %d JSON files, %d pass document filter", len(json_files), len(candidates))

    if args.dry_run:
        total_candidate_pages = 0
        for p in candidates:
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                pages = data.get("pages", [])
                n = sum(1 for pg in pages if is_candidate_page(pg, args.min_composite))
                if n:
                    rel = p
                    for root in args.input_roots:
                        try:
                            rel = p.relative_to(root)
                            break
                        except ValueError:
                            pass
                    print(f"  {n:3d} candidate pages  {rel}")
                    total_candidate_pages += n
            except Exception:
                pass
        print(f"\nTotal candidate pages: {total_candidate_pages}")
        return

    counts = {"enriched": 0, "no_candidates": 0, "skipped_error": 0,
              "no_pdf": 0, "unreadable": 0}
    total_pages_enriched = 0

    try:
        for json_path in candidates:
            rel = json_path
            for root in args.input_roots:
                try:
                    rel = json_path.relative_to(root)
                    break
                except ValueError:
                    pass

            status, pages_enriched, pages_skipped, elapsed = process_document(
                json_path, keywords, args.min_composite,
                args.force, args.vlm_model, args.vlm_url,
            )
            counts[status] = counts.get(status, 0) + 1
            total_pages_enriched += pages_enriched

            if status == "enriched" or pages_enriched > 0:
                log.info("%-70s  %d pages  %.1fs", rel, pages_enriched, elapsed)
            elif status not in ("no_candidates",):
                log.warning("%-70s  %s", rel, status)
    except KeyboardInterrupt:
        log.info("Interrupted.")

    log.info(
        "Done — enriched: %d docs (%d pages)  no_candidates: %d  errors: %d",
        counts.get("enriched", 0), total_pages_enriched,
        counts.get("no_candidates", 0),
        counts.get("no_pdf", 0) + counts.get("unreadable", 0),
    )


if __name__ == "__main__":
    main()

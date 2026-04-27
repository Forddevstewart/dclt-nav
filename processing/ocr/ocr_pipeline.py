"""
OCR Pipeline — process PDFs into keyword-scored text JSON.

For each input PDF at <input_root>/path/to/doc.pdf, produces a parallel
output at <output_root>/path/to/doc.json.

Usage:
    python3 -m processing.ocr.ocr_pipeline \\
        --input-root  /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter \\
        --output-root /Volumes/DigitalTwin/CivicTwin/ocr/agendacenter
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from pdf2image import convert_from_path
from PIL import Image
import pytesseract
from rapidfuzz.distance import Levenshtein as _Lev

log = logging.getLogger(__name__)

PIPELINE_VERSION = "0.1"
CONTEXT_WINDOW = 50       # chars on each side of a match
CONTEXT_SATURATION = 2    # unique context hits to reach score 1.0

COMPOSITE_WEIGHTS: dict[str, float] = {
    "exact_match":    0.35,
    "fuzzy_match":    0.25,
    "context":        0.15,
    "vlm_classifier": 0.25,
}


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class KeywordConfig:
    primary_terms: list[str]
    abbreviations: list[str] = field(default_factory=list)
    context_terms: list[str] = field(default_factory=list)


def load_keywords(path: Path) -> dict[str, KeywordConfig]:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return {
        name: KeywordConfig(
            primary_terms=entry.get("primary_terms", []),
            abbreviations=entry.get("abbreviations", []),
            context_terms=entry.get("context_terms", []),
        )
        for name, entry in raw.items()
    }


# ── Hash / skip ───────────────────────────────────────────────────────────────

def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def should_skip(
    output_path: Path,
    source_hash: str,
    reprocess_window: int,
) -> bool:
    """Return True only if output exists, hash matches, and is within the window."""
    if not output_path.exists():
        return False
    try:
        data = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if data.get("source_hash") != source_hash:
        return False
    ts = data.get("processed_at")
    if not ts:
        return False
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).days
    except Exception:
        return False
    return age < reprocess_window


# ── Image preprocessing ───────────────────────────────────────────────────────

def preprocess_image(img: Image.Image) -> Image.Image:
    """Deskew via Tesseract OSD, denoise and binarize via Sauvola (or fallback)."""
    # Deskew
    try:
        osd = pytesseract.image_to_osd(img, output_type=pytesseract.Output.DICT)
        angle = int(osd.get("rotate", 0))
        if angle:
            img = img.rotate(-angle, expand=True, fillcolor=255)
    except Exception:
        pass

    try:
        import numpy as np
        from skimage.color import rgb2gray
        from skimage.filters import threshold_sauvola

        arr = np.array(img.convert("RGB"))
        gray = rgb2gray(arr)
        thresh = threshold_sauvola(gray, window_size=25)
        binary = ((gray > thresh) * 255).astype(np.uint8)
        return Image.fromarray(binary)
    except ImportError:
        pass

    return img.convert("L")


# ── OCR engines ───────────────────────────────────────────────────────────────

def run_tesseract(img: Image.Image) -> str:
    return pytesseract.image_to_string(img, config="--psm 1")


_paddle: object = None  # lazy singleton


def run_paddleocr(img: Image.Image) -> str:
    global _paddle
    try:
        if _paddle is None:
            from paddleocr import PaddleOCR
            _paddle = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)
        import numpy as np
        result = _paddle.ocr(np.array(img.convert("RGB")), cls=True)
        if not result or not result[0]:
            return ""
        return "\n".join(line[1][0] for line in result[0] if line and line[1])
    except ImportError:
        return ""
    except Exception as e:
        log.debug("PaddleOCR error: %s", e)
        return ""


def run_vlm_transcription(img: Image.Image, model: str, ollama_url: str) -> str:
    try:
        import requests
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": "Transcribe all text on this page. Output only the text.",
                "images": [b64],
                "stream": False,
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        log.debug("VLM transcription error: %s", e)
        return ""


# ── Text utilities ────────────────────────────────────────────────────────────

def union_texts(*texts: str) -> str:
    """Concatenate OCR outputs, deduplicating identical lines while preserving order."""
    seen: set[str] = set()
    lines: list[str] = []
    for text in texts:
        for line in text.splitlines():
            stripped = line.rstrip()
            if stripped and stripped not in seen:
                seen.add(stripped)
                lines.append(stripped)
    return "\n".join(lines)


# ── Scoring ───────────────────────────────────────────────────────────────────

def score_exact_match(text: str, keyword: KeywordConfig) -> float:
    tl = text.lower()
    for term in keyword.primary_terms + keyword.abbreviations:
        if term.lower() in tl:
            return 1.0
    return 0.0


def score_fuzzy_match(text: str, keyword: KeywordConfig) -> float:
    words = text.lower().split()
    best = 0.0
    for term in keyword.primary_terms + keyword.abbreviations:
        term_l = term.lower()
        n = len(term_l.split())
        threshold = 3 if len(term) >= 6 else 1
        for i in range(max(1, len(words) - n + 1)):
            window = " ".join(words[i : i + n])
            dist = _Lev.distance(term_l, window, score_cutoff=threshold)
            if dist <= threshold:
                score = 1.0 - dist / max(len(term), 1)
                best = max(best, score)
                if best == 1.0:
                    return 1.0
    return best


def score_context(text: str, keyword: KeywordConfig) -> float:
    if not keyword.context_terms:
        return 0.0
    tl = text.lower()
    all_terms = keyword.primary_terms + keyword.abbreviations
    positions: list[int] = []
    for term in all_terms:
        start = 0
        while True:
            pos = tl.find(term.lower(), start)
            if pos == -1:
                break
            positions.append(pos)
            start = pos + 1
    if not positions:
        return 0.0
    found: set[str] = set()
    for pos in positions:
        window = tl[max(0, pos - CONTEXT_WINDOW) : pos + CONTEXT_WINDOW]
        for ctx in keyword.context_terms:
            if ctx.lower() in window:
                found.add(ctx.lower())
    return min(len(found) / CONTEXT_SATURATION, 1.0)


def score_vlm_classifier(
    img: Image.Image,
    keyword_name: str,
    model: str,
    ollama_url: str,
) -> Optional[float]:
    try:
        import requests
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        concept = keyword_name.replace("_", " ")
        prompt = (
            f'Does this page reference the concept of "{concept}"? '
            "Reply with a confidence from 0.0 to 1.0 followed by a brief explanation."
        )
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={"model": model, "prompt": prompt, "images": [b64], "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        text = resp.json().get("response", "")
        m = re.search(r"\b(1\.0|0\.\d+)\b", text)
        return float(m.group(1)) if m else None
    except Exception as e:
        log.debug("VLM classifier error for %s: %s", keyword_name, e)
        return None


def compute_composite(components: dict[str, Optional[float]]) -> float:
    available = {k: v for k, v in components.items() if v is not None}
    if not available:
        return 0.0
    total_weight = sum(COMPOSITE_WEIGHTS[k] for k in available)
    return sum(COMPOSITE_WEIGHTS[k] * v for k, v in available.items()) / total_weight


def score_keyword(
    text: str,
    img: Image.Image,
    keyword_name: str,
    keyword: KeywordConfig,
    use_vlm: bool,
    vlm_model: str,
    ollama_url: str,
) -> dict:
    components: dict[str, Optional[float]] = {
        "exact_match": score_exact_match(text, keyword),
        "fuzzy_match": score_fuzzy_match(text, keyword),
        "context":     score_context(text, keyword),
    }
    if use_vlm:
        components["vlm_classifier"] = score_vlm_classifier(
            img, keyword_name, vlm_model, ollama_url
        )
    composite = compute_composite(components)
    return {
        "composite": round(composite, 4),
        "components": {
            k: (round(v, 4) if v is not None else None)
            for k, v in components.items()
        },
    }


# ── Page processing ───────────────────────────────────────────────────────────

def process_page(
    img: Image.Image,
    page_number: int,
    keywords: dict[str, KeywordConfig],
    use_vlm: bool,
    vlm_model: str,
    ollama_url: str,
) -> dict:
    preprocessed = preprocess_image(img)

    tess_text   = run_tesseract(preprocessed)
    paddle_text = run_paddleocr(preprocessed)
    engines     = ["tesseract"]
    parts       = [tess_text]

    if paddle_text:
        engines.append("paddleocr")
        parts.append(paddle_text)

    if use_vlm:
        vlm_text = run_vlm_transcription(img, vlm_model, ollama_url)
        if vlm_text:
            engines.append("vlm")
            parts.append(vlm_text)

    unified = union_texts(*parts)

    return {
        "page_number": page_number,
        "text": unified,
        "engines_used": engines,
        "keyword_scores": {
            name: score_keyword(unified, img, name, kw, use_vlm, vlm_model, ollama_url)
            for name, kw in keywords.items()
        },
    }


# ── PDF processing ────────────────────────────────────────────────────────────

def process_pdf(
    pdf_path: Path,
    input_root: Path,
    keywords: dict[str, KeywordConfig],
    reprocess_window: int,
    force: bool,
    use_vlm: bool,
    vlm_model: str,
    ollama_url: str,
    workers: int,
) -> tuple[str, str, float]:
    """Process one PDF. Returns (status, rel_path, elapsed_seconds).
    Output JSON is written alongside the PDF: doc.pdf → doc.json.
    """
    t0 = time.monotonic()
    rel = pdf_path.relative_to(input_root)
    out = pdf_path.with_suffix(".json")

    try:
        # Reject non-PDF files saved with a .pdf extension
        with open(pdf_path, "rb") as _f:
            magic = _f.read(4)
        if magic != b"%PDF":
            log.warning("Not a PDF (magic=%r): %s — skipping", magic, rel)
            return "not_pdf", str(rel), time.monotonic() - t0

        source_hash = compute_sha256(pdf_path)

        if not force and should_skip(out, source_hash, reprocess_window):
            return "skipped", str(rel), time.monotonic() - t0

        images = convert_from_path(str(pdf_path), dpi=300)

        def _proc_page(idx_img: tuple[int, Image.Image]) -> tuple[int, dict]:
            idx, img = idx_img
            return idx, process_page(img, idx + 1, keywords, use_vlm, vlm_model, ollama_url)

        page_results: list[dict] = [{}] * len(images)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for idx, page_data in pool.map(_proc_page, enumerate(images)):
                page_results[idx] = page_data

        doc = {
            "source_path":      str(rel),
            "source_hash":      source_hash,
            "processed_at":     datetime.now(timezone.utc).isoformat(),
            "pipeline_version": PIPELINE_VERSION,
            "page_count":       len(images),
            "pages":            page_results,
        }

        tmp = out.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.rename(out)

        return "processed", str(rel), time.monotonic() - t0

    except Exception as exc:
        log.exception("Failed %s: %s", pdf_path, exc)
        try:
            tmp = out.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps({
                    "source_path":      str(rel),
                    "error":            str(exc),
                    "processed_at":     datetime.now(timezone.utc).isoformat(),
                    "pipeline_version": PIPELINE_VERSION,
                }, indent=2),
                encoding="utf-8",
            )
            tmp.rename(out)
        except Exception:
            pass
        return "failed", str(rel), time.monotonic() - t0


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="OCR pipeline — PDF → keyword-scored JSON")
    parser.add_argument("--input-root",        type=Path, required=True)
    parser.add_argument("--keywords",          type=Path,
                        default=Path(__file__).parent / "keywords.yaml")
    parser.add_argument("--reprocess-window",  type=int, default=30, metavar="DAYS")
    parser.add_argument("--force",             action="store_true")
    parser.add_argument("--use-vlm",           action="store_true")
    parser.add_argument("--vlm-model",         default="qwen2.5vl:7b")
    parser.add_argument("--vlm-url",           default="http://localhost:11434")
    parser.add_argument("--workers",           type=int, default=4)
    parser.add_argument("--dry-run",           action="store_true")
    parser.add_argument("--log-level",         default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    keywords = load_keywords(args.keywords)
    log.info("Loaded %d keyword groups from %s", len(keywords), args.keywords)

    pdfs = sorted(
        p for ext in ("*.pdf", "*.PDF") for p in args.input_root.rglob(ext)
    )
    log.info("Found %d PDFs under %s", len(pdfs), args.input_root)

    if args.dry_run:
        for pdf in pdfs:
            rel = pdf.relative_to(args.input_root)
            out = pdf.with_suffix(".json")
            tag = "skip" if (not args.force and out.exists()) else "process"
            print(f"  {tag}  {rel}")
        return

    counts: dict[str, int] = {"processed": 0, "skipped": 0, "failed": 0, "not_pdf": 0}
    try:
        for pdf in pdfs:
            status, rel, elapsed = process_pdf(
                pdf, args.input_root,
                keywords, args.reprocess_window, args.force,
                args.use_vlm, args.vlm_model, args.vlm_url, args.workers,
            )
            counts[status] += 1
            log.info("%-60s  %-9s  %.1fs", rel, status, elapsed)
    except KeyboardInterrupt:
        log.info("Interrupted.")

    log.info(
        "Done — processed: %d  skipped: %d  not_pdf: %d  failed: %d",
        counts["processed"], counts["skipped"], counts["not_pdf"], counts["failed"],
    )


if __name__ == "__main__":
    main()

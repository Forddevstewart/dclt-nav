import json
import re as _re
import urllib.request as _urlreq
from datetime import datetime
from flask import Blueprint, jsonify, send_file, redirect, abort
from .models import get_reference_db
from .db_utils import table_exists
from .keywords import KW_KEYS
from .civictwin_paths import registry_scan_path

bp = Blueprint("documents", __name__, url_prefix="/api")

REGISTRY_BASE = "https://search.barnstabledeeds.org"

_DOC_SKIP = {"_loaded_at"}

# In-process cache: viewer_url → resolved /WwwImg/ PDF URL.
# Avoids repeated outbound fetches to barnstabledeeds.org for the same document.
_rod_cache: dict[str, str] = {}


def _registry_viewer_url(doc) -> str | None:
    imid = (doc.get("image_id") or "").strip()
    if not imid:
        return None
    try:
        dt = datetime.strptime((doc.get("recorded_date") or "")[:10], "%Y-%m-%d")
        year, month, day = f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}"
    except (ValueError, TypeError):
        year = month = day = ""
    ctln = doc.get("document_number") or ""
    params = (
        f"WSIQTP=LR01I&W9RCCY={year}&W9RCMM={month}&W9RCDD={day}"
        f"&W9CTLN={ctln}&WSKYCD=B&W9IMID={imid}"
    )
    return f"{REGISTRY_BASE}/ALIS/WW400R.HTM?{params}"


def _fetch_rod_redirect(viewer_url: str):
    """Fetch the ALIS viewer page and return a redirect to the embedded PDF.

    The ALIS HTML viewer embeds the document as a /WwwImg/ path.  Redirecting
    to that path serves a bare PDF that browsers (and iframes) can display
    without hitting X-Frame-Options restrictions on the viewer wrapper page.
    Falls back to the viewer URL itself if extraction fails.

    Results are cached in _rod_cache so repeated requests for the same document
    (e.g. iframe reload, expand/collapse) resolve instantly without a second
    outbound fetch.
    """
    if viewer_url in _rod_cache:
        return redirect(_rod_cache[viewer_url])
    try:
        req = _urlreq.Request(viewer_url, headers={"User-Agent": "Mozilla/5.0"})
        with _urlreq.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("latin-1", errors="replace")
        paths = _re.findall(r'/WwwImg/[^\s"\'<>#]+\.PDF', html, _re.IGNORECASE)
        if paths:
            page_suffix = _re.compile(r'\d{4}\.PDF$', _re.IGNORECASE)
            base_paths = [p for p in paths if not page_suffix.search(p)]
            chosen = base_paths[0] if base_paths else paths[0]
            pdf_url = REGISTRY_BASE + chosen
            _rod_cache[viewer_url] = pdf_url
            return redirect(pdf_url)
    except Exception:
        pass
    _rod_cache[viewer_url] = viewer_url  # cache fallback too
    return redirect(viewer_url)


# ── SQL builders ──────────────────────────────────────────────────────────────

def _document_list_sql(has_ocr: bool) -> str:
    """Build the document list SELECT. One row per unique book/page."""
    if has_ocr:
        kw_cols = ",\n".join(
            f"    COALESCE(o.kw_{kw}, 0.0) kw_{kw}" for kw in KW_KEYS
        )
        return f"""
            SELECT
                d.book, d.page, MIN(d.parcel_id) parcel_id,
                d.instrument_type, d.recorded_date,
                d.grantor, d.grantee, d.address, d.scan_cached, d.doc_amount,
                {kw_cols},
                CASE WHEN o.book IS NOT NULL THEN 1 ELSE 0 END has_ocr
            FROM registry_documents d
            LEFT JOIN registry_ocr o ON d.book = o.book AND d.page = o.page
            GROUP BY d.book, d.page
            ORDER BY d.recorded_date DESC
        """
    zero_cols = ",\n".join(f"    0.0 kw_{kw}" for kw in KW_KEYS)
    return f"""
        SELECT
            book, page, MIN(parcel_id) parcel_id,
            instrument_type, recorded_date,
            grantor, grantee, address, scan_cached, doc_amount,
            {zero_cols},
            0 has_ocr
        FROM registry_documents
        GROUP BY book, page
        ORDER BY recorded_date DESC
    """


def _fetch_document_row(db, book, page):
    """Fetch a single registry_documents row, or None."""
    return db.execute(
        "SELECT * FROM registry_documents WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()


def _fetch_ocr_row(db, book, page):
    """Fetch OCR row for a document, or None if table absent."""
    if not table_exists(db, "registry_ocr"):
        return None
    return db.execute(
        "SELECT * FROM registry_ocr WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()


# ── Document list ─────────────────────────────────────────────────────────────

@bp.route("/documents")
def documents_list():
    db = get_reference_db()
    has_ocr = table_exists(db, "registry_ocr")
    sql = _document_list_sql(has_ocr)
    rows = db.execute(sql).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


# ── Document detail ───────────────────────────────────────────────────────────

@bp.route("/documents/<book>/<page>")
def document_detail(book, page):
    db = get_reference_db()

    doc = _fetch_document_row(db, book, page)
    if not doc:
        db.close()
        abort(404)

    ocr = _fetch_ocr_row(db, book, page)
    db.close()

    doc_dict = {k: v for k, v in dict(doc).items() if k not in _DOC_SKIP}
    try:
        doc_dict["cross_refs"] = json.loads(doc_dict.get("cross_refs") or "[]")
    except (ValueError, TypeError):
        doc_dict["cross_refs"] = []
    doc_dict["alis_url"] = _registry_viewer_url(doc_dict)

    ocr_dict = None
    if ocr:
        ocr_dict = {k: v for k, v in dict(ocr).items() if k not in {"_loaded_at", "source_hash"}}

    return jsonify({
        "document": doc_dict,
        "ocr":      ocr_dict,
    })


# ── Registry of Deeds direct PDF redirect ────────────────────────────────────

@bp.route("/documents/<book>/<page>/rod")
def document_rod(book, page):
    """Redirect to the raw PDF on the Registry of Deeds server."""
    db = get_reference_db()
    doc = _fetch_document_row(db, book, page)
    db.close()

    if not doc:
        abort(404)

    viewer_url = _registry_viewer_url(dict(doc))
    if not viewer_url:
        abort(404)

    return _fetch_rod_redirect(viewer_url)


# ── Document PDF ──────────────────────────────────────────────────────────────

@bp.route("/documents/<book>/<page>/pdf")
def document_pdf(book, page):
    db = get_reference_db()
    doc = _fetch_document_row(db, book, page)
    db.close()

    if not doc:
        abort(404)

    doc = dict(doc)

    if doc.get("scan_cached"):
        path = registry_scan_path(book, page)
        if path.exists():
            return send_file(path, mimetype="application/pdf")

    viewer_url = _registry_viewer_url(doc)
    if not viewer_url:
        abort(404)

    return _fetch_rod_redirect(viewer_url)

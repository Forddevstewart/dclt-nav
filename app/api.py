from datetime import datetime
from flask import Blueprint, jsonify, send_file, redirect, abort
from .models import get_all_items, get_raw_db

bp = Blueprint("api", __name__, url_prefix="/api")

REGISTRY_BASE = "https://search.barnstabledeeds.org"


def _registry_viewer_url(doc) -> str | None:
    imid = (doc["image_id"] or "").strip()
    if not imid:
        return None
    try:
        dt = datetime.strptime((doc["recorded_date"] or "")[:10], "%Y-%m-%d")
        year, month, day = f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}"
    except (ValueError, TypeError):
        year = month = day = ""
    ctln = doc["document_number"] or ""
    params = (
        f"WSIQTP=LR01I&W9RCCY={year}&W9RCMM={month}&W9RCDD={day}"
        f"&W9CTLN={ctln}&WSKYCD=B&W9IMID={imid}"
    )
    return f"{REGISTRY_BASE}/ALIS/WW400R.HTM?{params}"


@bp.route("/items")
def items():
    return jsonify(get_all_items())


@bp.route("/documents/<book>/<page>/pdf")
def document_pdf(book, page):
    db = get_raw_db()
    doc = db.execute(
        "SELECT * FROM registry_documents WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()
    db.close()

    if not doc:
        abort(404)

    if doc["scan_cached"]:
        from discovery.registry.cache import scan_path
        path = scan_path(book, page)
        if path.exists():
            return send_file(path, mimetype="application/pdf")

    url = _registry_viewer_url(doc)
    if url:
        return redirect(url)

    abort(404)

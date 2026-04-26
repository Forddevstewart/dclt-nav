import json
from datetime import datetime
from flask import Blueprint, jsonify, send_file, redirect, abort
from .models import get_all_items, get_reference_db

bp = Blueprint("api", __name__, url_prefix="/api")

REGISTRY_BASE = "https://search.barnstabledeeds.org"

KW_KEYS = [
    "conservation_restriction",
    "article_97",
    "deed_restriction",
    "chapter_61",
    "agricultural_preservation_restriction",
    "perpetual_restriction",
    "ccr",
]
KW_LABELS = {
    "conservation_restriction":             "Conservation Restriction",
    "article_97":                           "Article 97",
    "deed_restriction":                     "Deed Restriction",
    "chapter_61":                           "Chapter 61",
    "agricultural_preservation_restriction":"Ag. Preservation Restriction",
    "perpetual_restriction":                "Perpetual Restriction",
    "ccr":                                  "CC&R",
}

_PARCEL_SKIP = {"_loaded_at", "backbone_source", "join_status"}
_DOC_SKIP    = {"_loaded_at"}
_GIS_SKIP    = {"parcel_id", "_loaded_at"}
_SOIL_SKIP   = {"parcel_id", "_loaded_at"}

GIS_LAYER_COLS = [
    ("Zone 1 WHP",               "zone1_type",   False),
    ("Zone 2 WHP",               "zone2_id",     False),
    ("Priority Habitat",         "prihab_id",    False),
    ("Estimated Habitat",        "esthab_id",    False),
    ("Natural Community",        "natcomm_id",   False),
    ("BioMap3 Vernal Pool",      "bm3_vp_id",    False),
    ("BioMap3 Wetland Corridor", "bm3_wc_id",    False),
    ("BioMap3 Core Habitat",     "bm3_ch_id",    False),
    ("BioMap3 CNL",              "bm3_cnl_id",   False),
    ("Open Space",               "os_site_name", False),
    ("Wetlands",                 "wetlands_code",False),
    ("Structures",               "struct_count", True),   # numeric > 0
]


def _table_exists(db, name: str) -> bool:
    return db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()[0] > 0


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


def _clean(row: dict, skip: set) -> dict:
    return {k: v for k, v in row.items() if k not in skip}


# ── Items (legacy) ────────────────────────────────────────────────────────────

@bp.route("/items")
def items():
    return jsonify(get_all_items())


# ── Overview ──────────────────────────────────────────────────────────────────

@bp.route("/overview")
def overview():
    db = get_reference_db()

    def cnt(sql):
        return db.execute(sql).fetchone()[0]

    def brk(sql):
        return [dict(r) for r in db.execute(sql).fetchall()]

    has_gis      = _table_exists(db, "parcels_gis")
    has_ocr      = _table_exists(db, "registry_ocr")
    has_sources  = _table_exists(db, "gis_sources")

    # GIS layer coverage
    layer_cov = []
    if has_gis:
        for label, col, is_numeric in GIS_LAYER_COLS:
            if is_numeric:
                n = cnt(f"SELECT COUNT(*) FROM parcels_gis WHERE {col} > 0")
            else:
                n = cnt(f"SELECT COUNT(*) FROM parcels_gis WHERE {col} IS NOT NULL AND {col} != ''")
            layer_cov.append({"layer": label, "n": n})

    # OCR keyword hit counts (threshold 0.4)
    kw_hits = {}
    if has_ocr:
        for kw in KW_KEYS:
            kw_hits[kw] = {
                "label": KW_LABELS[kw],
                "n_02": cnt(f"SELECT COUNT(*) FROM registry_ocr WHERE kw_{kw} > 0.2"),
                "n_04": cnt(f"SELECT COUNT(*) FROM registry_ocr WHERE kw_{kw} > 0.4"),
            }

    last_run = brk(
        "SELECT stage, run_at FROM _pipeline_runs ORDER BY run_id DESC LIMIT 1"
    )

    result = {
        "pipeline": {
            "last_run": last_run[0] if last_run else None,
        },
        "registry": {
            "documents":   cnt("SELECT COUNT(*) FROM registry_documents"),
            "scan_cached": cnt("SELECT COUNT(*) FROM registry_documents WHERE scan_cached=1"),
            "ocr":         cnt("SELECT COUNT(*) FROM registry_ocr") if has_ocr else 0,
            "by_type":     brk(
                "SELECT instrument_type, COUNT(*) n FROM registry_documents"
                " GROUP BY instrument_type ORDER BY n DESC LIMIT 12"
            ),
            "kw_hits": kw_hits,
        },
        "parcels": {
            "total":    cnt("SELECT COUNT(*) FROM parcels"),
            "by_class": brk(
                "SELECT property_class, COUNT(*) n FROM parcels"
                " GROUP BY property_class ORDER BY n DESC"
            ),
        },
        "assessor": {
            "records": cnt("SELECT COUNT(*) FROM assessor"),
        },
        "massgis": {
            "raw":        cnt("SELECT COUNT(*) FROM massgis"),
            "normalized": cnt("SELECT COUNT(*) FROM layer_massgis"),
        },
        "gis": {
            "total_parcels": cnt("SELECT COUNT(*) FROM parcels_gis") if has_gis else 0,
            "layer_coverage": layer_cov,
            "sources": cnt("SELECT COUNT(*) FROM gis_sources") if has_sources else 0,
        },
        "warrants": {
            "total":      cnt("SELECT COUNT(*) FROM warrants"),
            "year_range": brk("SELECT MIN(year) min_year, MAX(year) max_year FROM warrants")[0],
            "by_result":  brk(
                "SELECT result, COUNT(*) n FROM warrants"
                " WHERE result IS NOT NULL AND result != ''"
                " GROUP BY result ORDER BY n DESC"
            ),
        },
        "reference": {
            "use_codes":      cnt("SELECT COUNT(*) FROM ref_use_codes"),
            "schema_columns": cnt("SELECT COUNT(*) FROM schema_columns"),
        },
    }

    db.close()
    return jsonify(result)


# ── Parcels ───────────────────────────────────────────────────────────────────

@bp.route("/parcels")
def parcels_list():
    db = get_reference_db()
    rows = db.execute(
        "SELECT parcel_id, site_addr, owner_name, owner_category,"
        "       property_class, use_code_norm, use_code_desc,"
        "       totalapprvalue, billingacres, village, is_public, condo_units"
        " FROM parcels ORDER BY site_addr"
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/parcels/<parcel_id>")
def parcel_detail(parcel_id):
    db = get_reference_db()

    parcel = db.execute(
        "SELECT * FROM parcels WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()
    if not parcel:
        db.close()
        abort(404)

    docs = db.execute(
        "SELECT * FROM registry_documents WHERE parcel_id = ? ORDER BY doc_rank",
        (parcel_id,),
    ).fetchall()

    gis = db.execute(
        "SELECT * FROM parcels_gis WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()

    soil = db.execute(
        "SELECT * FROM layer_soils WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()

    db.close()

    doc_list = []
    for d in docs:
        rec = _clean(dict(d), _DOC_SKIP)
        try:
            rec["cross_refs"] = json.loads(rec.get("cross_refs") or "[]")
        except (ValueError, TypeError):
            rec["cross_refs"] = []
        rec["alis_url"] = _registry_viewer_url(rec)
        doc_list.append(rec)

    return jsonify({
        "parcel":    _clean(dict(parcel), _PARCEL_SKIP),
        "documents": doc_list,
        "gis":       _clean(dict(gis), _GIS_SKIP) if gis else None,
        "soil":      _clean(dict(soil), _SOIL_SKIP) if soil else None,
    })


# ── Documents list ────────────────────────────────────────────────────────────

@bp.route("/documents")
def documents_list():
    db = get_reference_db()
    has_ocr = _table_exists(db, "registry_ocr")

    kw_cols = ",\n".join(
        f"    COALESCE(o.kw_{kw}, 0.0) kw_{kw}" for kw in KW_KEYS
    )
    has_ocr_col = "CASE WHEN o.book IS NOT NULL THEN 1 ELSE 0 END has_ocr"

    # One row per unique document (book/page). registry_documents has multiple
    # rows per deed when the same doc is associated with multiple parcels;
    # duplicate keys in that payload break Alpine's x-for rendering.
    if has_ocr:
        sql = f"""
            SELECT
                d.book, d.page, MIN(d.parcel_id) parcel_id,
                d.instrument_type, d.recorded_date,
                d.grantor, d.grantee, d.address, d.scan_cached, d.doc_amount,
                {kw_cols},
                {has_ocr_col}
            FROM registry_documents d
            LEFT JOIN registry_ocr o ON d.book = o.book AND d.page = o.page
            GROUP BY d.book, d.page
            ORDER BY d.recorded_date DESC
        """
    else:
        zero_cols = ",\n".join(f"    0.0 kw_{kw}" for kw in KW_KEYS)
        sql = f"""
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

    rows = db.execute(sql).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


# ── Document detail ───────────────────────────────────────────────────────────

@bp.route("/documents/<book>/<page>")
def document_detail(book, page):
    db = get_reference_db()

    doc = db.execute(
        "SELECT * FROM registry_documents WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()
    if not doc:
        db.close()
        abort(404)

    ocr = None
    if _table_exists(db, "registry_ocr"):
        ocr = db.execute(
            "SELECT * FROM registry_ocr WHERE book = ? AND page = ? LIMIT 1",
            (book, page),
        ).fetchone()

    db.close()

    doc_dict = _clean(dict(doc), _DOC_SKIP)
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
    """Redirect to the raw PDF on the Registry of Deeds server.

    Fetches the ALIS HTML viewer, extracts the /WwwImg/ PDF path, and
    redirects the browser directly to the file. Falls back to the viewer
    URL if the path can't be extracted (e.g. doc requires cart payment).
    """
    import re
    import urllib.request as _urlreq

    db = get_reference_db()
    doc = db.execute(
        "SELECT * FROM registry_documents WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()
    db.close()

    if not doc:
        abort(404)

    viewer_url = _registry_viewer_url(dict(doc))
    if not viewer_url:
        abort(404)

    try:
        req = _urlreq.Request(viewer_url, headers={"User-Agent": "Mozilla/5.0"})
        with _urlreq.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("latin-1", errors="replace")

        paths = re.findall(r'/WwwImg/[^\s"\'<>#]+\.PDF', html, re.IGNORECASE)
        if paths:
            page_suffix = re.compile(r'\d{4}\.PDF$', re.IGNORECASE)
            base_paths = [p for p in paths if not page_suffix.search(p)]
            chosen = base_paths[0] if base_paths else paths[0]
            return redirect(REGISTRY_BASE + chosen)
    except Exception:
        pass

    return redirect(viewer_url)


# ── Document PDF ──────────────────────────────────────────────────────────────

@bp.route("/documents/<book>/<page>/pdf")
def document_pdf(book, page):
    db = get_reference_db()
    doc = db.execute(
        "SELECT * FROM registry_documents WHERE book = ? AND page = ? LIMIT 1",
        (book, page),
    ).fetchone()
    db.close()

    if not doc:
        abort(404)

    doc = dict(doc)

    if doc.get("scan_cached"):
        from discovery.registry.cache import scan_path
        path = scan_path(book, page)
        if path.exists():
            return send_file(path, mimetype="application/pdf")

    url = _registry_viewer_url(doc)
    if url:
        return redirect(url)

    abort(404)

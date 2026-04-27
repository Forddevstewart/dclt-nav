import json
from datetime import datetime
from pathlib import Path
from flask import Blueprint, jsonify, send_file, redirect, abort, request
from flask_login import current_user, login_required
from .models import get_all_items, get_reference_db, get_db

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
    has_gis      = _table_exists(db, "parcels_gis")
    has_ocr      = _table_exists(db, "registry_ocr") and _table_exists(db, "registry_documents")
    has_for_sale = _table_exists(db, "layer_for_sale")

    if has_gis:
        gis_select = """,
            CASE WHEN g.wetlands_code IS NOT NULL AND g.wetlands_code !='' THEN 1 ELSE 0 END has_wetlands,
            CASE WHEN g.zone1_type    IS NOT NULL AND g.zone1_type    !='' THEN 1 ELSE 0 END has_zone1,
            CASE WHEN g.zone2_id      IS NOT NULL AND g.zone2_id      !='' THEN 1 ELSE 0 END has_zone2,
            CASE WHEN g.prihab_id     IS NOT NULL AND g.prihab_id     !='' THEN 1 ELSE 0 END has_prihab,
            CASE WHEN g.esthab_id     IS NOT NULL AND g.esthab_id     !='' THEN 1 ELSE 0 END has_esthab,
            CASE WHEN g.natcomm_id    IS NOT NULL AND g.natcomm_id    !='' THEN 1 ELSE 0 END has_natcomm,
            CASE WHEN (g.bm3_vp_id  IS NOT NULL AND g.bm3_vp_id !='')
                   OR (g.bm3_wc_id  IS NOT NULL AND g.bm3_wc_id !='')
                   OR (g.bm3_ch_id  IS NOT NULL AND g.bm3_ch_id !='')
                   OR (g.bm3_cnl_id IS NOT NULL AND g.bm3_cnl_id!='') THEN 1 ELSE 0 END has_bm3,
            CASE WHEN g.os_site_name  IS NOT NULL AND g.os_site_name  !='' THEN 1 ELSE 0 END has_openspace"""
        gis_join = "LEFT JOIN parcels_gis g ON g.parcel_id = p.parcel_id"
    else:
        gis_select = ", 0 has_wetlands, 0 has_zone1, 0 has_zone2, 0 has_prihab, 0 has_esthab, 0 has_natcomm, 0 has_bm3, 0 has_openspace"
        gis_join = ""

    if has_ocr:
        kw_select = "".join(
            f",\n            COALESCE(kw.kw_{k}, 0) kw_{k}" for k in KW_KEYS
        )
        kw_agg = ",\n                   ".join(
            f"MAX(CASE WHEN ro.kw_{k} > 0.4 THEN 1 ELSE 0 END) kw_{k}" for k in KW_KEYS
        )
        kw_join = f"""LEFT JOIN (
            SELECT rd.parcel_id,
                   {kw_agg}
            FROM registry_documents rd
            JOIN registry_ocr ro ON ro.book = rd.book AND ro.page = rd.page
            GROUP BY rd.parcel_id
        ) kw ON kw.parcel_id = p.parcel_id"""
    else:
        kw_select = "".join(f", 0 kw_{k}" for k in KW_KEYS)
        kw_join = ""

    if has_for_sale:
        fs_select = ", CASE WHEN fs.norm_address IS NOT NULL THEN 1 ELSE 0 END for_sale"
        fs_join = (
            "LEFT JOIN layer_for_sale fs"
            " ON p.locno IS NOT NULL AND p.locno != ''"
            " AND p.locst IS NOT NULL AND p.locst != ''"
            " AND UPPER(fs.norm_address) LIKE printf('%d', CAST(p.locno AS REAL))||' '||UPPER(p.locst)||'%'"
        )
    else:
        fs_select = ", 0 for_sale"
        fs_join = ""

    sql = f"""
        SELECT p.parcel_id, p.site_addr, p.owner_name, p.owner_category,
               p.property_class, p.use_code_norm, p.use_code_desc,
               p.totalapprvalue, p.billingacres, p.village, p.is_public, p.condo_units,
               p.centroid_lat
               {gis_select}{kw_select}{fs_select}
        FROM parcels p
        {gis_join}
        {kw_join}
        {fs_join}
        ORDER BY p.locst NULLS LAST, p.locno NULLS LAST
    """
    rows = db.execute(sql).fetchall()
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


# ── Parcel geometry ───────────────────────────────────────────────────────────

_geojson_index: dict | None = None


def _get_geojson_index() -> dict:
    global _geojson_index
    if _geojson_index is not None:
        return _geojson_index
    from discovery.config import get_config
    cfg = get_config()
    gis_files = cfg.collection_files("gis")
    path = Path(gis_files[0]["abs_path"]) if gis_files else cfg.root / "gis" / "dennis_parcels.geojson"
    if not path.exists():
        _geojson_index = {}
        return _geojson_index
    data = json.loads(path.read_text())
    _geojson_index = {
        f["properties"].get("MAP_PAR_ID"): f
        for f in data.get("features", [])
        if f.get("properties", {}).get("MAP_PAR_ID")
    }
    return _geojson_index


@bp.route("/parcels/<parcel_id>/geometry")
def parcel_geometry(parcel_id):
    idx = _get_geojson_index()
    feature = idx.get(parcel_id)
    if not feature:
        abort(404)
    return jsonify(feature)


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


# ── Town docs ─────────────────────────────────────────────────────────────────

@bp.route("/town-docs/overview")
def town_docs_overview():
    db = get_reference_db()
    if not _table_exists(db, "town_docs"):
        db.close()
        return jsonify({"total": 0, "by_committee": [], "by_doc_type": []})

    total = db.execute("SELECT COUNT(*) FROM town_docs").fetchone()[0]
    by_committee = [
        dict(r) for r in db.execute(
            "SELECT committee, COUNT(*) n FROM town_docs"
            " GROUP BY committee ORDER BY n DESC"
        ).fetchall()
    ]
    by_doc_type = [
        dict(r) for r in db.execute(
            "SELECT doc_type, COUNT(*) n FROM town_docs"
            " GROUP BY doc_type ORDER BY n DESC"
        ).fetchall()
    ]
    db.close()
    return jsonify({"total": total, "by_committee": by_committee, "by_doc_type": by_doc_type})


@bp.route("/town-docs")
def town_docs_list():
    """List town docs that have at least one candidate link, with candidate counts."""
    ref  = get_reference_db()
    dclt = get_db()

    if not _table_exists(ref, "town_docs"):
        ref.close(); dclt.close()
        return jsonify([])

    has_links = _table_exists(dclt, "parcel_links")
    committee = request.args.get("committee", "")
    status    = request.args.get("status", "")   # 'candidate','confirmed','rejected','' = all with candidates

    where_td = "WHERE full_text IS NOT NULL AND full_text != ''"
    params: list = []
    if committee:
        where_td += " AND committee = ?"
        params.append(committee)

    if has_links:
        rows_td = ref.execute(
            f"SELECT doc_id, source_type, committee, doc_type, meeting_date, page_count"
            f" FROM town_docs {where_td} ORDER BY meeting_date DESC NULLS LAST, committee",
            params[:1] if committee else [],
        ).fetchall()

        doc_ids = [r["doc_id"] for r in rows_td]
        link_counts: dict[str, dict] = {}
        if doc_ids:
            placeholders = ",".join("?" * len(doc_ids))
            link_rows = dclt.execute(
                f"SELECT doc_id, status, COUNT(*) n FROM parcel_links"
                f" WHERE doc_id IN ({placeholders}) GROUP BY doc_id, status",
                doc_ids,
            ).fetchall()
            for lr in link_rows:
                lc = link_counts.setdefault(lr["doc_id"], {"n_candidate": 0, "n_confirmed": 0, "n_rejected": 0})
                lc[f"n_{lr['status']}"] = lr["n"]

        # Deduplicate by (committee, meeting_date): prefer 'Updated' doc_type,
        # summing link counts from both so no hygiene work is hidden.
        seen_key: dict[tuple, int] = {}  # (committee, meeting_date) -> index in result
        result = []
        for r in rows_td:
            lc     = link_counts.get(r["doc_id"], {})
            n_cand = lc.get("n_candidate", 0)
            n_conf = lc.get("n_confirmed", 0)
            n_rej  = lc.get("n_rejected",  0)
            n_total = n_cand + n_conf + n_rej
            if n_total == 0:
                continue   # skip docs with no links at all
            if status and lc.get(f"n_{status}", 0) == 0:
                continue   # status filter: skip docs without that bucket
            key = (r["committee"], r["meeting_date"])
            if key in seen_key:
                idx = seen_key[key]
                existing = result[idx]
                existing["n_candidate"] += n_cand
                existing["n_confirmed"] += n_conf
                existing["n_rejected"]  += n_rej
                if r["doc_type"] == "Updated":
                    # promote to the Updated doc_id so detail view shows the right doc
                    existing.update({k: r[k] for k in ("doc_id", "source_type", "doc_type", "page_count")})
            else:
                row = dict(r)
                row.update({"n_candidate": n_cand, "n_confirmed": n_conf, "n_rejected": n_rej})
                seen_key[key] = len(result)
                result.append(row)
    else:
        rows_td = ref.execute(
            f"SELECT doc_id, source_type, committee, doc_type, meeting_date, page_count"
            f" FROM town_docs {where_td} ORDER BY meeting_date DESC NULLS LAST, committee",
            params[:1] if committee else [],
        ).fetchall()
        result = [dict(r) | {"n_candidate": 0, "n_confirmed": 0, "n_rejected": 0}
                  for r in rows_td]

    ref.close(); dclt.close()
    return jsonify(result)


@bp.route("/town-docs/<path:doc_id>")
def town_doc_detail(doc_id):
    ref  = get_reference_db()
    dclt = get_db()

    doc = ref.execute(
        "SELECT * FROM town_docs WHERE doc_id = ? LIMIT 1", (doc_id,)
    ).fetchone()
    if not doc:
        ref.close(); dclt.close()
        abort(404)

    links = []
    if _table_exists(dclt, "parcel_links"):
        links = [dict(r) for r in dclt.execute(
            "SELECT link_id, parcel_id, match_type, match_text, confidence, status,"
            "       reviewed_by, reviewed_at, created_at"
            " FROM parcel_links WHERE doc_id = ?"
            " ORDER BY confidence DESC, parcel_id",
            (doc_id,),
        ).fetchall()]

    ref.close(); dclt.close()
    return jsonify({"doc": dict(doc), "links": links})


# ── Data Hygiene — parcel link adjudication ───────────────────────────────────

@bp.route("/hygiene/links/<int:link_id>", methods=["PATCH"])
@login_required
def hygiene_update_link(link_id):
    data   = request.get_json() or {}
    status = data.get("status", "")
    if status not in ("candidate", "confirmed", "rejected"):
        return jsonify({"error": "status must be candidate, confirmed, or rejected"}), 400

    db = get_db()
    row = db.execute("SELECT link_id FROM parcel_links WHERE link_id = ?", (link_id,)).fetchone()
    if not row:
        db.close()
        return jsonify({"error": "not found"}), 404

    db.execute(
        "UPDATE parcel_links SET status=?, reviewed_by=?, reviewed_at=datetime('now') WHERE link_id=?",
        (status, current_user.id, link_id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})


@bp.route("/hygiene/links", methods=["POST"])
@login_required
def hygiene_create_link():
    """Create a manual confirmed link (user-picked parcel not detected by OCR)."""
    data        = request.get_json() or {}
    doc_id      = (data.get("doc_id") or "").strip()
    parcel_id   = (data.get("parcel_id") or "").strip()
    source_type = (data.get("source_type") or "agendacenter").strip()

    if not doc_id or not parcel_id:
        return jsonify({"error": "doc_id and parcel_id required"}), 400

    db = get_db()
    try:
        cur = db.execute(
            """INSERT INTO parcel_links (doc_id, source_type, parcel_id, match_type, confidence,
                   status, reviewed_by, reviewed_at)
               VALUES (?, ?, ?, 'user_manual', 1.0, 'confirmed', ?, datetime('now'))
               ON CONFLICT(doc_id, parcel_id) DO UPDATE SET
                   status='confirmed', match_type='user_manual',
                   reviewed_by=excluded.reviewed_by, reviewed_at=excluded.reviewed_at""",
            (doc_id, source_type, parcel_id, current_user.id),
        )
        db.commit()
        link_id = cur.lastrowid
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500
    db.close()
    return jsonify({"ok": True, "link_id": link_id}), 201


@bp.route("/hygiene/links/<int:link_id>", methods=["DELETE"])
@login_required
def hygiene_delete_link(link_id):
    db = get_db()
    row = db.execute("SELECT link_id, status FROM parcel_links WHERE link_id = ?", (link_id,)).fetchone()
    if not row:
        db.close()
        return jsonify({"error": "not found"}), 404
    db.execute("DELETE FROM parcel_links WHERE link_id = ?", (link_id,))
    db.commit()
    db.close()
    return jsonify({"ok": True})


@bp.route("/town-docs/<path:doc_id>/pdf")
def town_doc_pdf(doc_id):
    ref = get_reference_db()
    doc = ref.execute(
        "SELECT source_path FROM town_docs WHERE doc_id = ? LIMIT 1", (doc_id,)
    ).fetchone()
    ref.close()
    if not doc or not doc["source_path"]:
        abort(404)
    from discovery.config import get_config
    pdf_path = get_config().root / "ma-dennis" / doc["source_path"]
    if not pdf_path.exists():
        abort(404)
    return send_file(pdf_path, mimetype="application/pdf")


@bp.route("/parcels/<parcel_id>/town-docs")
def parcel_town_docs(parcel_id):
    """Confirmed town doc links for a parcel, with doc metadata from reference.db."""
    dclt = get_db()
    ref  = get_reference_db()

    if not _table_exists(dclt, "parcel_links"):
        dclt.close(); ref.close()
        return jsonify([])

    links = dclt.execute(
        "SELECT link_id, doc_id, source_type, match_type, confidence, created_at"
        " FROM parcel_links WHERE parcel_id = ? AND status = 'confirmed'"
        " ORDER BY created_at DESC",
        (parcel_id,),
    ).fetchall()

    result = []
    seen = {}  # (committee, meeting_date) -> index in result; prefer 'Updated' doc_type
    for lk in links:
        doc = ref.execute(
            "SELECT committee, doc_type, meeting_date, source_path, page_count"
            " FROM town_docs WHERE doc_id = ? LIMIT 1",
            (lk["doc_id"],),
        ).fetchone()
        row = dict(lk)
        if doc:
            row.update(dict(doc))
        key = (row.get("committee"), row.get("meeting_date"))
        if key in seen:
            if row.get("doc_type") == "Updated":
                result[seen[key]] = row
        else:
            seen[key] = len(result)
            result.append(row)

    dclt.close(); ref.close()
    return jsonify(result)

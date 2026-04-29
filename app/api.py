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


def _column_exists(db, table: str, column: str) -> bool:
    return any(
        row[1] == column
        for row in db.execute(f"PRAGMA table_info({table})")
    )


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
            "by_type": (
                brk(
                    "SELECT CASE"
                    "  WHEN rd.instrument_type GLOB '[0-9]*'"
                    "    OR UPPER(rd.instrument_type) LIKE 'LOT%'"
                    "  THEN 'Parcel-specific'"
                    "  ELSE rd.instrument_type END AS instrument_type,"
                    " COUNT(*) AS enumerated,"
                    " SUM(rd.scan_cached) AS downloaded,"
                    " COUNT(ro.book) AS ocr"
                    " FROM registry_documents rd"
                    " LEFT JOIN registry_ocr ro ON rd.book = ro.book AND rd.page = ro.page"
                    " GROUP BY 1"
                    " ORDER BY enumerated DESC"
                ) if has_ocr else brk(
                    "SELECT CASE"
                    "  WHEN instrument_type GLOB '[0-9]*'"
                    "    OR UPPER(instrument_type) LIKE 'LOT%'"
                    "  THEN 'Parcel-specific'"
                    "  ELSE instrument_type END AS instrument_type,"
                    " COUNT(*) AS enumerated,"
                    " SUM(scan_cached) AS downloaded,"
                    " 0 AS ocr"
                    " FROM registry_documents"
                    " GROUP BY 1"
                    " ORDER BY enumerated DESC"
                )
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
    has_coverage = _column_exists(db, "parcels", "coverage_ratio")

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
        fs_select = (
            ", CASE WHEN p.locno IS NOT NULL AND p.locno != ''"
            " AND p.locst IS NOT NULL AND p.locst != ''"
            " AND EXISTS (SELECT 1 FROM layer_for_sale"
            "  WHERE UPPER(norm_address) LIKE printf('%d', CAST(p.locno AS REAL))||' '||UPPER(p.locst)||'%')"
            " THEN 1 ELSE 0 END for_sale"
        )
        fs_join = ""
    else:
        fs_select = ", 0 for_sale"
        fs_join = ""

    cov_select = ", p.coverage_ratio, p.coverage_status" if has_coverage else ", NULL coverage_ratio, NULL coverage_status"

    sql = f"""
        SELECT p.parcel_id, p.site_addr, p.owner_name, p.owner_category,
               p.property_class, p.use_code_norm, p.use_code_desc,
               p.totalapprvalue, p.billingacres, p.village, p.is_public, p.condo_units,
               p.centroid_lat{cov_select}
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


def _link_counts(cand_parcel_ids: set, adj_map: dict) -> tuple[int, int, int]:
    """Return (n_candidate, n_confirmed, n_rejected) for a single doc."""
    n_confirmed = sum(1 for s in adj_map.values() if s in ("confirmed", "user_manual"))
    n_rejected  = sum(1 for s in adj_map.values() if s == "rejected")
    n_candidate = len(cand_parcel_ids - set(adj_map.keys()))
    return n_candidate, n_confirmed, n_rejected


@bp.route("/town-docs")
def town_docs_list():
    """List town docs that have at least one candidate link, with candidate counts."""
    from collections import defaultdict

    ref  = get_reference_db()
    dclt = get_db()

    if not _table_exists(ref, "town_docs"):
        ref.close(); dclt.close()
        return jsonify([])

    has_candidates = _table_exists(ref, "parcel_link_candidates")
    committee = request.args.get("committee", "")
    status    = request.args.get("status", "")

    where_td = "WHERE full_text IS NOT NULL AND full_text != ''"
    params: list = []
    if committee:
        where_td += " AND committee = ?"
        params.append(committee)

    rows_td = ref.execute(
        f"SELECT doc_id, source_type, committee, doc_type, meeting_date, page_count"
        f" FROM town_docs {where_td} ORDER BY meeting_date DESC NULLS LAST, committee",
        params,
    ).fetchall()

    if not has_candidates:
        ref.close(); dclt.close()
        result = [dict(r) | {"n_candidate": 0, "n_confirmed": 0, "n_rejected": 0}
                  for r in rows_td]
        return jsonify(result)

    doc_ids = [r["doc_id"] for r in rows_td]
    if not doc_ids:
        ref.close(); dclt.close()
        return jsonify([])

    placeholders = ",".join("?" * len(doc_ids))

    # Candidates from reference.db: doc_id -> set of parcel_ids
    cand_by_doc: dict[str, set] = defaultdict(set)
    for r in ref.execute(
        f"SELECT doc_id, parcel_id FROM parcel_link_candidates WHERE doc_id IN ({placeholders})",
        doc_ids,
    ).fetchall():
        cand_by_doc[r["doc_id"]].add(r["parcel_id"])

    # Adjudications from dclt.db: doc_id -> {parcel_id: status}
    adj_by_doc: dict[str, dict] = defaultdict(dict)
    if _table_exists(dclt, "parcel_link_adjudications"):
        for r in dclt.execute(
            f"SELECT doc_id, parcel_id, status FROM parcel_link_adjudications"
            f" WHERE doc_id IN ({placeholders})",
            doc_ids,
        ).fetchall():
            adj_by_doc[r["doc_id"]][r["parcel_id"]] = r["status"]

    seen_key: dict[tuple, int] = {}
    result = []
    for r in rows_td:
        did = r["doc_id"]
        cands = cand_by_doc.get(did, set())
        adjs  = adj_by_doc.get(did, {})
        n_cand, n_conf, n_rej = _link_counts(cands, adjs)
        if n_cand + n_conf + n_rej == 0:
            continue

        key = (r["committee"], r["meeting_date"])
        if key in seen_key:
            idx = seen_key[key]
            existing = result[idx]
            # Union parcel sets to avoid double-counting parcels in both original and Updated
            merged_cands = existing["_cands"] | cands
            merged_adjs  = {**existing["_adjs"], **adjs}
            m_cand, m_conf, m_rej = _link_counts(merged_cands, merged_adjs)
            existing["_cands"]      = merged_cands
            existing["_adjs"]       = merged_adjs
            existing["n_candidate"] = m_cand
            existing["n_confirmed"] = m_conf
            existing["n_rejected"]  = m_rej
            if r["doc_type"] == "Updated":
                existing.update({k: r[k] for k in ("doc_id", "source_type", "doc_type", "page_count")})
        else:
            row = dict(r)
            row.update({"n_candidate": n_cand, "n_confirmed": n_conf, "n_rejected": n_rej,
                        "_cands": cands, "_adjs": adjs})
            seen_key[key] = len(result)
            result.append(row)

    # Apply status filter and strip internal tracking fields
    out = []
    for row in result:
        row.pop("_cands"); row.pop("_adjs")
        if status == "candidate" and row["n_candidate"] == 0: continue
        if status == "confirmed" and row["n_confirmed"] == 0: continue
        if status == "rejected"  and row["n_rejected"]  == 0: continue
        out.append(row)

    ref.close(); dclt.close()
    return jsonify(out)


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

    # Candidates from reference.db
    candidates = []
    if _table_exists(ref, "parcel_link_candidates"):
        candidates = ref.execute(
            "SELECT parcel_id, source_type, match_type, match_text, confidence"
            " FROM parcel_link_candidates WHERE doc_id = ?"
            " ORDER BY confidence DESC, parcel_id",
            (doc_id,),
        ).fetchall()

    # Adjudications from dclt.db
    adj_map: dict[str, dict] = {}
    if _table_exists(dclt, "parcel_link_adjudications"):
        for r in dclt.execute(
            "SELECT parcel_id, status, reviewed_by, reviewed_at"
            " FROM parcel_link_adjudications WHERE doc_id = ?",
            (doc_id,),
        ).fetchall():
            adj_map[r["parcel_id"]] = dict(r)

    cand_pids = {c["parcel_id"] for c in candidates}
    links = []
    for c in candidates:
        pid = c["parcel_id"]
        adj = adj_map.get(pid, {})
        links.append({
            "link_id":     doc_id + "|" + pid,
            "parcel_id":   pid,
            "source_type": c["source_type"],
            "match_type":  c["match_type"],
            "match_text":  c["match_text"],
            "confidence":  c["confidence"],
            "status":      adj.get("status", "candidate"),
            "reviewed_by": adj.get("reviewed_by"),
            "reviewed_at": adj.get("reviewed_at"),
        })

    # User-manual adjudications with no pipeline candidate
    for pid, adj in adj_map.items():
        if pid not in cand_pids and adj["status"] == "user_manual":
            links.append({
                "link_id":     doc_id + "|" + pid,
                "parcel_id":   pid,
                "source_type": adj.get("source_type"),
                "match_type":  "user_manual",
                "match_text":  None,
                "confidence":  1.0,
                "status":      "user_manual",
                "reviewed_by": adj.get("reviewed_by"),
                "reviewed_at": adj.get("reviewed_at"),
            })

    ref.close(); dclt.close()
    return jsonify({"doc": dict(doc), "links": links})


# ── Data Hygiene — parcel link adjudication ───────────────────────────────────

@bp.route("/hygiene/links/<path:link_id>", methods=["PATCH"])
@login_required
def hygiene_update_link(link_id):
    data   = request.get_json() or {}
    status = data.get("status", "")
    if status not in ("candidate", "confirmed", "rejected"):
        return jsonify({"error": "status must be candidate, confirmed, or rejected"}), 400

    try:
        doc_id, parcel_id = link_id.rsplit("|", 1)
    except ValueError:
        return jsonify({"error": "invalid link_id"}), 400

    db = get_db()
    if status == "candidate":
        # Revert to unreviewed — delete the adjudication row
        db.execute(
            "DELETE FROM parcel_link_adjudications WHERE doc_id = ? AND parcel_id = ?",
            (doc_id, parcel_id),
        )
    else:
        db.execute(
            """INSERT INTO parcel_link_adjudications (doc_id, parcel_id, status, reviewed_by, reviewed_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(doc_id, parcel_id) DO UPDATE SET
                   status=excluded.status, reviewed_by=excluded.reviewed_by,
                   reviewed_at=excluded.reviewed_at""",
            (doc_id, parcel_id, status, current_user.id),
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
        db.execute(
            """INSERT INTO parcel_link_adjudications
                   (doc_id, parcel_id, status, source_type, match_type, confidence, reviewed_by, reviewed_at)
               VALUES (?, ?, 'user_manual', ?, 'user_manual', 1.0, ?, datetime('now'))
               ON CONFLICT(doc_id, parcel_id) DO UPDATE SET
                   status='user_manual', match_type='user_manual',
                   reviewed_by=excluded.reviewed_by, reviewed_at=excluded.reviewed_at""",
            (doc_id, parcel_id, source_type, current_user.id),
        )
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500
    db.close()
    return jsonify({"ok": True, "link_id": doc_id + "|" + parcel_id}), 201


@bp.route("/hygiene/links/<path:link_id>", methods=["DELETE"])
@login_required
def hygiene_delete_link(link_id):
    try:
        doc_id, parcel_id = link_id.rsplit("|", 1)
    except ValueError:
        return jsonify({"error": "invalid link_id"}), 400

    db = get_db()
    rows_affected = db.execute(
        "DELETE FROM parcel_link_adjudications WHERE doc_id = ? AND parcel_id = ?",
        (doc_id, parcel_id),
    ).rowcount
    db.commit()
    db.close()
    if not rows_affected:
        return jsonify({"error": "not found"}), 404
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

    if not _table_exists(dclt, "parcel_link_adjudications"):
        dclt.close(); ref.close()
        return jsonify([])

    links = dclt.execute(
        "SELECT doc_id, source_type, match_type, confidence, reviewed_at"
        " FROM parcel_link_adjudications"
        " WHERE parcel_id = ? AND status IN ('confirmed', 'user_manual')"
        " ORDER BY reviewed_at DESC",
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
        row = {"link_id": lk["doc_id"] + "|" + parcel_id}
        row.update(dict(lk))
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

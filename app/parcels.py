import json
from pathlib import Path
from flask import Blueprint, jsonify, abort
from flask_login import login_required
from .models import get_reference_db, get_db
from .db_utils import table_exists, column_exists, FOLD as _FOLD
from .dimensions import JOIN_STATUS_TO_IDENTITY_STATE, DIMENSIONS
from discovery.keywords import KW_KEYS

bp = Blueprint("parcels", __name__, url_prefix="/api")

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

_PARCEL_SKIP = {"_loaded_at", "backbone_source"}
_GIS_SKIP    = {"parcel_id", "_loaded_at"}
_SOIL_SKIP   = {"parcel_id", "_loaded_at"}


def _clean(row: dict, skip: set) -> dict:
    return {k: v for k, v in row.items() if k not in skip}


# ── Items (legacy) ────────────────────────────────────────────────────────────

@bp.route("/items")
def items():
    from .models import get_all_items
    return jsonify(get_all_items())


# ── Parcel list ───────────────────────────────────────────────────────────────

@bp.route("/parcels")
def parcels_list():
    db = get_reference_db()
    has_gis      = table_exists(db, "parcels_gis")
    has_ocr      = table_exists(db, "registry_ocr") and table_exists(db, "registry_documents")
    has_for_sale = table_exists(db, "layer_for_sale")
    has_coverage  = column_exists(db, "parcels", "coverage_ratio")
    has_usc       = column_exists(db, "parcels", "is_undeveloped_state_code")
    has_farming   = column_exists(db, "parcels", "farming_suitability")
    has_acq_suit  = column_exists(db, "parcels", "acquisition_suitability")

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
    else:
        fs_select = ", 0 for_sale"

    cov_select      = ", p.coverage_ratio, p.coverage_status" if has_coverage else ", NULL coverage_ratio, NULL coverage_status"
    usc_select      = ", p.is_undeveloped_state_code" if has_usc else ", 0 is_undeveloped_state_code"
    farming_select  = ", p.farming_suitability" if has_farming else ", NULL farming_suitability"
    acq_suit_select = ", p.acquisition_suitability" if has_acq_suit else ", NULL acquisition_suitability"

    sql = f"""
        SELECT p.parcel_id, p.site_addr, p.owner_name, p.owner_category,
               p.property_class, p.use_code_norm, p.use_code_desc,
               p.totalapprvalue, p.billingacres, p.village, p.is_public, p.condo_units,
               p.centroid_lat,
               p.parcel_class, p.parcel_gisid_status, p.parcel_massgis_status,
               p.parcel_adb_gisid,
               CASE
                 WHEN p.parcel_class = 'special-feature' THEN 'OK'
                 WHEN p.join_status = 'BOTH'             THEN 'OK'
                 WHEN p.join_status = 'ASSESSOR_ONLY'    THEN 'ADB-only'
                 WHEN p.join_status = 'MASSGIS_ONLY'     THEN 'GIS-only'
                 ELSE 'OK'
               END AS identity_state{cov_select}{usc_select}{farming_select}{acq_suit_select}
               {gis_select}{kw_select}{fs_select}
        FROM parcels p
        {gis_join}
        {kw_join}
        ORDER BY p.locst NULLS LAST, p.locno NULLS LAST
    """
    rows = db.execute(sql).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


# ── Parcel detail ─────────────────────────────────────────────────────────────

@bp.route("/parcels/<parcel_id>")
def parcel_detail(parcel_id):
    ref = get_reference_db()
    tx  = get_db()

    parcel = ref.execute(
        "SELECT * FROM parcels WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()
    if not parcel:
        ref.close(); tx.close()
        abort(404)

    docs = ref.execute(
        "SELECT * FROM registry_documents WHERE parcel_id = ? ORDER BY doc_rank",
        (parcel_id,),
    ).fetchall()

    gis = ref.execute(
        "SELECT * FROM parcels_gis WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()

    soil = ref.execute(
        "SELECT * FROM layer_soils WHERE parcel_id = ? LIMIT 1", (parcel_id,)
    ).fetchone()

    # Tags: current fold state + applicability for all parcel dimensions.
    tag_rows = tx.execute(
        "SELECT tag_id, name FROM tags"
        " WHERE deprecated_at IS NULL AND target_entity IN ('parcel','any')"
        " ORDER BY display_order",
    ).fetchall()
    fold_rows = tx.execute(
        f"SELECT t1.tag_id, t1.state FROM taggings t1"
        f" WHERE t1.target_type = 'parcel' AND t1.target_id = ? AND {_FOLD}",
        (parcel_id,),
    ).fetchall()
    fold_map = {r["tag_id"]: r["state"] for r in fold_rows}

    tags_block = {}
    for tr in tag_rows:
        dim_name = tr["name"]
        dim = DIMENSIONS.get(dim_name)
        if dim is None or dim.node_type != "parcel":
            continue
        applicable, reason = dim.is_applicable("parcel", parcel_id, ref)
        tags_block[dim_name] = {
            "tag_id":     tr["tag_id"],
            "state":      fold_map.get(tr["tag_id"]),
            "applicable": applicable,
            "reason":     reason if not applicable else "",
        }

    ref.close()
    tx.close()

    doc_list = []
    for d in docs:
        rec = _clean(dict(d), {"_loaded_at"})
        try:
            rec["cross_refs"] = json.loads(rec.get("cross_refs") or "[]")
        except (ValueError, TypeError):
            rec["cross_refs"] = []
        from .documents import _registry_viewer_url
        rec["alis_url"] = _registry_viewer_url(rec)
        doc_list.append(rec)

    parcel_dict = _clean(dict(parcel), _PARCEL_SKIP)
    p = dict(parcel)
    if p.get("parcel_class") == "special-feature":
        parcel_dict["identity_state"] = "OK"
    else:
        parcel_dict["identity_state"] = JOIN_STATUS_TO_IDENTITY_STATE.get(p.get("join_status"), "OK")

    return jsonify({
        "parcel":    parcel_dict,
        "documents": doc_list,
        "gis":       _clean(dict(gis), _GIS_SKIP) if gis else None,
        "soil":      _clean(dict(soil), _SOIL_SKIP) if soil else None,
        "tags":      tags_block,
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


# ── Parcel → linked town docs ─────────────────────────────────────────────────

@bp.route("/parcels/<parcel_id>/town-docs")
def parcel_town_docs(parcel_id):
    """Confirmed town doc links for a parcel, with doc metadata from reference.db."""
    dclt = get_db()
    ref  = get_reference_db()

    if not table_exists(dclt, "parcel_link_adjudications"):
        dclt.close(); ref.close()
        return jsonify([])

    links = dclt.execute(
        "SELECT doc_id, source_type, match_type, confidence, reviewed_at"
        " FROM parcel_link_adjudications"
        " WHERE parcel_id = ?"
        " AND seq IN ("
        "  SELECT MAX(seq) FROM parcel_link_adjudications"
        "  WHERE parcel_id = ? GROUP BY doc_id, parcel_id"
        " )"
        " AND status IN ('confirmed', 'user_manual')"
        " ORDER BY reviewed_at DESC",
        (parcel_id, parcel_id),
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

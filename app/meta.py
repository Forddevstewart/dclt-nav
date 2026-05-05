from flask import Blueprint, jsonify, request, abort
from .models import get_reference_db, get_db
from .db_utils import table_exists
from discovery.keywords import KW_KEYS, KW_LABELS
from .parcels import GIS_LAYER_COLS

bp = Blueprint("meta", __name__, url_prefix="/api")

_DYNAMIC_LAYERS = {
    "parcel-coverage-rollup":  "parcel_coverage_rollup",
    "parcel-article97-rollup": "parcel_article97_rollup",
}


# ── Overview ──────────────────────────────────────────────────────────────────

@bp.route("/overview")
def overview():
    db = get_reference_db()

    def cnt(sql):
        return db.execute(sql).fetchone()[0]

    def brk(sql):
        return [dict(r) for r in db.execute(sql).fetchall()]

    has_gis      = table_exists(db, "parcels_gis")
    has_ocr      = table_exists(db, "registry_ocr")
    has_sources  = table_exists(db, "gis_sources")

    layer_cov = []
    if has_gis:
        for label, col, is_numeric in GIS_LAYER_COLS:
            if is_numeric:
                n = cnt(f"SELECT COUNT(*) FROM parcels_gis WHERE {col} > 0")
            else:
                n = cnt(f"SELECT COUNT(*) FROM parcels_gis WHERE {col} IS NOT NULL AND {col} != ''")
            layer_cov.append({"layer": label, "n": n})

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


# ── Dynamic Layers ────────────────────────────────────────────────────────────

@bp.route("/layers/<layer_name>")
def layer_eval(layer_name):
    """Evaluate a Dynamic Layer by name and return its current value."""
    if layer_name not in _DYNAMIC_LAYERS:
        abort(404, f"layer '{layer_name}' not found")
    from . import layers as _layers
    fn = getattr(_layers, _DYNAMIC_LAYERS[layer_name])
    return jsonify(fn())


# ── Filter entry points ───────────────────────────────────────────────────────

@bp.route("/filter-entry-points")
def filter_entry_points_list():
    """Curated deep-link entry points for a node type.

    ?node_type=parcel  — optional; returns all node types if omitted.
    Only non-deprecated entries are returned, ordered by display_order.
    """
    node_type = request.args.get("node_type", "")
    db = get_db()
    if not table_exists(db, "filter_entry_points"):
        db.close()
        return jsonify([])
    if node_type:
        rows = db.execute(
            "SELECT entry_point_id, node_type, label, group_label,"
            "       payload_json, display_order"
            " FROM filter_entry_points"
            " WHERE deprecated_at IS NULL AND node_type = ?"
            " ORDER BY display_order, entry_point_id",
            (node_type,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT entry_point_id, node_type, label, group_label,"
            "       payload_json, display_order"
            " FROM filter_entry_points"
            " WHERE deprecated_at IS NULL"
            " ORDER BY node_type, display_order, entry_point_id",
        ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

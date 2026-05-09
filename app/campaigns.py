"""Campaign endpoints.

Campaigns are portal-authored work packages that drive a Tag dimension to
terminus. Each campaign names a set of dimensions, highlight attributes,
and relevant document types.

Progress (/api/campaigns/progress) returns fold state distribution for all
non-deprecated dimensions — the authoritative measure of campaign completeness.
"""

from flask import Blueprint, jsonify, request, abort
from flask_login import login_required

from .models import get_db, get_reference_db
from .db_utils import table_exists, FOLD as _FOLD
from .dimensions import DIMENSIONS

bp = Blueprint("campaigns", __name__, url_prefix="/api/campaigns")


# Applicable-set SQL per dimension. Reference.db is read-only; failures
# (missing columns) are caught and treated as total = 0.
_DIM_TOTAL_SQL = {
    "CoverageDetermination": (
        "parcel",
        "SELECT COUNT(*) FROM parcels",
    ),
    "FarmingDetermination": (
        "parcel",
        "SELECT COUNT(*) FROM parcels",
    ),
    "IdentityResolution": (
        "parcel",
        "SELECT COUNT(*) FROM parcels WHERE join_status != 'BOTH'",
    ),
    "AcquisitionDetermination": (
        "parcel",
        "SELECT COUNT(*) FROM parcels"
        " WHERE acquisition_suitability IN ('Possible','Likely')",
    ),
    "Article97Determination": (
        "document",
        "SELECT COUNT(*) FROM registry_documents rd"
        " JOIN registry_ocr ro ON ro.book = rd.book AND ro.page = rd.page"
        " WHERE ro.kw_article_97 > 0.4",
    ),
}


# ── Progress ──────────────────────────────────────────────────────────────────

@bp.route("/progress")
@login_required
def progress():
    """Fold state distribution for all non-deprecated tag dimensions.

    Returns a dict keyed by dimension name. Each entry carries total
    (applicable parcel/document count), by_state (array of {state, n, pct}),
    and node_type.
    """
    db  = get_db()
    ref = get_reference_db()

    tag_rows = db.execute(
        "SELECT tag_id, name, target_entity, states_csv FROM tags"
        " WHERE deprecated_at IS NULL"
        " ORDER BY display_order",
    ).fetchall()

    result = {}
    for tr in tag_rows:
        tag_name = tr["name"]
        dim = DIMENSIONS.get(tag_name)
        if dim is None:
            continue

        node_type, total_sql = _DIM_TOTAL_SQL.get(
            tag_name, (dim.node_type, None)
        )
        if total_sql is None:
            continue

        try:
            total = ref.execute(total_sql).fetchone()[0]
        except Exception:
            total = 0

        states = tr["states_csv"].split(",")
        default_state = states[0]

        counts_rows = db.execute(
            f"SELECT t1.state, COUNT(*) n FROM taggings t1"
            f" WHERE t1.target_type = ? AND t1.tag_id = ?"
            f"   AND t1.state IS NOT NULL AND {_FOLD}"
            f" GROUP BY t1.state",
            (node_type, tr["tag_id"]),
        ).fetchall()
        counts = {r["state"]: r["n"] for r in counts_rows}

        n_non_default = sum(v for s, v in counts.items() if s != default_state)
        n_default = max(0, total - n_non_default)

        by_state = []
        for s in states:
            n = n_default if s == default_state else counts.get(s, 0)
            by_state.append({
                "state": s,
                "n":     n,
                "pct":   round(n / total * 100, 1) if total else 0.0,
            })

        result[tag_name] = {
            "tag_id":    tr["tag_id"],
            "node_type": node_type,
            "total":     total,
            "by_state":  by_state,
        }

    db.close()
    ref.close()
    return jsonify(result)


# ── Campaign CRUD ─────────────────────────────────────────────────────────────

@bp.route("")
@login_required
def list_campaigns():
    db = get_db()
    if not table_exists(db, "campaigns"):
        db.close()
        return jsonify([])
    rows = db.execute(
        "SELECT campaign_id, name, description, status, created_at"
        " FROM campaigns WHERE status != 'complete'"
        " ORDER BY created_at DESC",
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/all")
@login_required
def list_all_campaigns():
    """All campaigns including complete, for admin view."""
    db = get_db()
    if not table_exists(db, "campaigns"):
        db.close()
        return jsonify([])
    rows = db.execute(
        "SELECT campaign_id, name, description, status, created_at"
        " FROM campaigns ORDER BY created_at DESC",
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/<campaign_id>")
@login_required
def campaign_detail(campaign_id):
    db = get_db()
    row = db.execute(
        "SELECT * FROM campaigns WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()
    if not row:
        db.close()
        abort(404)
    dims   = db.execute(
        "SELECT dimension FROM campaign_dimensions WHERE campaign_id = ?",
        (campaign_id,),
    ).fetchall()
    attrs  = db.execute(
        "SELECT attr_id, display_order FROM campaign_attributes"
        " WHERE campaign_id = ? ORDER BY display_order NULLS LAST, attr_id",
        (campaign_id,),
    ).fetchall()
    dtypes = db.execute(
        "SELECT doc_type FROM campaign_doc_types WHERE campaign_id = ?",
        (campaign_id,),
    ).fetchall()
    db.close()
    result = dict(row)
    result["dimensions"] = [r["dimension"] for r in dims]
    result["attributes"] = [r["attr_id"]   for r in attrs]
    result["doc_types"]  = [r["doc_type"]  for r in dtypes]
    return jsonify(result)


@bp.route("", methods=["POST"])
@login_required
def create_campaign():
    data        = request.get_json(force=True) or {}
    campaign_id = str(data.get("campaign_id", "")).strip()
    name        = str(data.get("name", "")).strip()
    if not campaign_id or not name:
        abort(400, "campaign_id and name are required")
    db = get_db()
    try:
        db.execute(
            "INSERT INTO campaigns (campaign_id, name, description) VALUES (?, ?, ?)",
            (campaign_id, name, data.get("description", "")),
        )
        for dim in data.get("dimensions", []):
            db.execute(
                "INSERT OR IGNORE INTO campaign_dimensions"
                " (campaign_id, dimension) VALUES (?, ?)",
                (campaign_id, dim),
            )
        for i, attr_id in enumerate(data.get("attributes", [])):
            db.execute(
                "INSERT OR IGNORE INTO campaign_attributes"
                " (campaign_id, attr_id, display_order) VALUES (?, ?, ?)",
                (campaign_id, attr_id, i * 10),
            )
        for doc_type in data.get("doc_types", []):
            db.execute(
                "INSERT OR IGNORE INTO campaign_doc_types"
                " (campaign_id, doc_type) VALUES (?, ?)",
                (campaign_id, doc_type),
            )
        db.commit()
    except Exception as e:
        db.close()
        abort(400, str(e))
    db.close()
    return jsonify({"campaign_id": campaign_id}), 201


@bp.route("/<campaign_id>/status", methods=["PATCH"])
@login_required
def update_status(campaign_id):
    data   = request.get_json(force=True) or {}
    status = str(data.get("status", "")).strip()
    if status not in ("active", "paused", "complete"):
        abort(400, "status must be active, paused, or complete")
    db = get_db()
    r  = db.execute(
        "UPDATE campaigns SET status = ? WHERE campaign_id = ?",
        (status, campaign_id),
    )
    db.commit()
    if r.rowcount == 0:
        db.close()
        abort(404)
    db.close()
    return jsonify({"campaign_id": campaign_id, "status": status})

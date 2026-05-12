"""Campaign endpoints.

Campaigns are portal-authored work packages that drive a Tag dimension to
terminus. Each campaign names a set of dimensions, highlight attributes,
and relevant document types.

Progress (/api/campaigns/progress) returns fold state distribution for all
non-deprecated dimensions — the authoritative measure of campaign completeness.

The canonical campaign definitions live in CAMPAIGNS below. seed_campaigns()
is called on app startup to upsert them into the DB with their metadata.
Admin users can adjust status and display_order; other fields are code-owned.
"""

from flask import Blueprint, jsonify, request, abort
from flask_login import login_required

from .models import get_db, get_reference_db
from .db_utils import table_exists, FOLD as _FOLD
from .dimensions import DIMENSIONS

bp = Blueprint("campaigns", __name__, url_prefix="/api/campaigns")


# ── Canonical campaign definitions ────────────────────────────────────────────

CAMPAIGNS = [
    {
        "campaign_id": "coverage_determination",
        "name":        "Coverage Determination",
        "label":       "Coverage",
        "description": "Determine whether each parcel falls within the conservation district's coverage area.",
        "scope":       "All parcels",
        "color":       "#10b981",
        "display_order": 10,
        "dimensions":  ["CoverageDetermination"],
        "doc_types":   [],
    },
    {
        "campaign_id": "farming_determination",
        "name":        "Farming Determination",
        "label":       "Farming",
        "description": "Identify parcels with active agricultural use.",
        "scope":       "All parcels",
        "color":       "#84cc16",
        "display_order": 20,
        "dimensions":  ["FarmingDetermination"],
        "doc_types":   [],
    },
    {
        "campaign_id": "identity_resolution",
        "name":        "Identity Resolution",
        "label":       "Identity",
        "description": "Resolve parcel identity where assessor and MassGIS records diverge.",
        "scope":       "Parcels where IdentityState ≠ OK",
        "color":       "#f59e0b",
        "display_order": 30,
        "dimensions":  ["IdentityResolution"],
        "doc_types":   [],
    },
    {
        "campaign_id": "acquisition_determination",
        "name":        "Acquisition Determination",
        "label":       "Acquisition",
        "description": "Evaluate parcels for acquisition suitability.",
        "scope":       "Parcels with Possible or Likely suitability",
        "color":       "#3b82f6",
        "display_order": 40,
        "dimensions":  ["AcquisitionDetermination"],
        "doc_types":   [],
    },
    {
        "campaign_id": "article97_determination",
        "name":        "Article 97 Determination",
        "label":       "Article 97",
        "description": "Flag registry documents referencing Article 97 protected land.",
        "scope":       "Documents with keyword score > 40%",
        "color":       "#8b5cf6",
        "display_order": 50,
        "dimensions":  ["Article97Determination"],
        "doc_types":   ["registry"],
    },
]


def seed_campaigns(db):
    """Upsert canonical campaigns into the DB. Only touches is_system=1 rows."""
    for c in CAMPAIGNS:
        db.execute(
            """INSERT INTO campaigns
                 (campaign_id, name, description, label, scope, color, display_order, is_system)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)
               ON CONFLICT(campaign_id) DO UPDATE SET
                 name          = excluded.name,
                 description   = excluded.description,
                 label         = excluded.label,
                 scope         = excluded.scope,
                 color         = excluded.color,
                 is_system     = 1
               WHERE is_system = 1""",
            (c["campaign_id"], c["name"], c["description"],
             c["label"], c["scope"], c["color"], c["display_order"]),
        )
        for dim in c["dimensions"]:
            db.execute(
                "INSERT OR IGNORE INTO campaign_dimensions (campaign_id, dimension) VALUES (?, ?)",
                (c["campaign_id"], dim),
            )
        for doc_type in c["doc_types"]:
            db.execute(
                "INSERT OR IGNORE INTO campaign_doc_types (campaign_id, doc_type) VALUES (?, ?)",
                (c["campaign_id"], doc_type),
            )
    db.commit()


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
        "SELECT COUNT(*) FROM parcels WHERE join_status != 'BOTH' AND parcel_class != 'special-feature'",
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


# ── Campaign meta (dimension → campaign display info) ─────────────────────────

@bp.route("/meta")
@login_required
def campaign_meta():
    """Return display metadata keyed by dimension name.

    Used by the frontend campaignMeta(tag) helper to look up label/scope/color
    for each tag dimension without hard-coding them in the JS.
    """
    db = get_db()
    if not table_exists(db, "campaigns"):
        db.close()
        return jsonify({})
    rows = db.execute(
        "SELECT c.campaign_id, c.label, c.scope, c.color, c.display_order, cd.dimension"
        " FROM campaigns c"
        " JOIN campaign_dimensions cd ON cd.campaign_id = c.campaign_id"
        " ORDER BY c.display_order",
    ).fetchall()
    db.close()
    result = {}
    for r in rows:
        result[r["dimension"]] = {
            "campaign_id":   r["campaign_id"],
            "label":         r["label"],
            "scope":         r["scope"],
            "color":         r["color"],
            "display_order": r["display_order"],
        }
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
        "SELECT campaign_id, name, label, description, status, color, display_order, created_at"
        " FROM campaigns WHERE status != 'complete'"
        " ORDER BY display_order, created_at",
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
        "SELECT campaign_id, name, label, description, status, color, scope, display_order, is_system, created_at"
        " FROM campaigns ORDER BY display_order, created_at",
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


@bp.route("/<campaign_id>/priority", methods=["PATCH"])
@login_required
def update_priority(campaign_id):
    """Swap display_order with the adjacent campaign (direction: up or down)."""
    data      = request.get_json(force=True) or {}
    direction = data.get("direction", "")
    if direction not in ("up", "down"):
        abort(400, "direction must be up or down")

    db = get_db()
    campaigns = db.execute(
        "SELECT campaign_id, display_order FROM campaigns ORDER BY display_order, created_at",
    ).fetchall()
    ids = [r["campaign_id"] for r in campaigns]
    orders = {r["campaign_id"]: r["display_order"] for r in campaigns}

    try:
        idx = ids.index(campaign_id)
    except ValueError:
        db.close()
        abort(404)

    swap_idx = idx - 1 if direction == "up" else idx + 1
    if swap_idx < 0 or swap_idx >= len(ids):
        db.close()
        return jsonify({"ok": False, "reason": "already at boundary"})

    swap_id = ids[swap_idx]
    db.execute(
        "UPDATE campaigns SET display_order = ? WHERE campaign_id = ?",
        (orders[swap_id], campaign_id),
    )
    db.execute(
        "UPDATE campaigns SET display_order = ? WHERE campaign_id = ?",
        (orders[campaign_id], swap_id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})

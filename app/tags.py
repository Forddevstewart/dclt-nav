from flask import Blueprint, jsonify, request, abort
from flask_login import login_required, current_user
from .models import get_db

_VALID_ENTITIES = {"parcel", "document"}

bp = Blueprint("tags", __name__, url_prefix="/api")

_FOLD = """
    t1.event_id = (
        SELECT MAX(t2.event_id) FROM taggings t2
        WHERE t2.tag_id      = t1.tag_id
          AND t2.target_type = t1.target_type
          AND t2.target_id   = t1.target_id
    )
"""


@bp.route("/tags")
def list_tags():
    """All non-deprecated tags for the picker.

    Optional ?entity=parcel|document filters to tags applicable to that
    entity type (target_entity matches or is 'any').
    """
    entity = request.args.get("entity")
    db = get_db()
    if entity and entity in _VALID_ENTITIES:
        rows = db.execute(
            "SELECT tag_id, name, tag_type, target_entity, states_csv, display_order"
            " FROM tags WHERE deprecated_at IS NULL"
            " AND (target_entity = ? OR target_entity = 'any')"
            " ORDER BY display_order, tag_id",
            (entity,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT tag_id, name, tag_type, target_entity, states_csv, display_order"
            " FROM tags WHERE deprecated_at IS NULL"
            " ORDER BY display_order, tag_id"
        ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/tagged/<entity_type>")
def tagged_entities(entity_type):
    """Return target_ids where ALL specified tags are applied (AND logic).

    ?tag_ids=1,2,3  — comma-separated tag_ids (required)
    ?threshold=0.4  — confidence floor for system tags (default 0.4)
    """
    if entity_type not in _VALID_ENTITIES:
        abort(400, "entity_type must be 'parcel' or 'document'")

    raw = request.args.get("tag_ids", "")
    try:
        tag_ids = [int(x) for x in raw.split(",") if x.strip()]
    except ValueError:
        abort(400, "invalid tag_ids")
    if not tag_ids:
        return jsonify([])

    try:
        threshold = float(request.args.get("threshold", 0.4))
    except ValueError:
        threshold = 0.4

    db = get_db()
    placeholders = ",".join("?" for _ in tag_ids)
    tag_rows = db.execute(
        f"SELECT tag_id, tag_type FROM tags WHERE tag_id IN ({placeholders})",
        tag_ids,
    ).fetchall()
    tag_type_map = {r["tag_id"]: r["tag_type"] for r in tag_rows}

    result_sets = []
    for tid in tag_ids:
        tag_type = tag_type_map.get(tid, "user")
        if tag_type == "system":
            rows = db.execute(
                f"SELECT DISTINCT t1.target_id FROM taggings t1"
                f" WHERE t1.target_type = ? AND t1.tag_id = ?"
                f"   AND t1.confidence >= ?"
                f"   AND {_FOLD}",
                (entity_type, tid, threshold),
            ).fetchall()
        else:
            rows = db.execute(
                f"SELECT DISTINCT t1.target_id FROM taggings t1"
                f" WHERE t1.target_type = ? AND t1.tag_id = ?"
                f"   AND t1.state IS NOT NULL"
                f"   AND {_FOLD}",
                (entity_type, tid),
            ).fetchall()
        result_sets.append({r["target_id"] for r in rows})

    db.close()

    if not result_sets:
        return jsonify([])
    combined = result_sets[0]
    for s in result_sets[1:]:
        combined &= s
    return jsonify(sorted(combined))


@bp.route("/tagging/<target_type>/<path:target_id>")
def tagging_for_target(target_type, target_id):
    """Non-deprecated tags plus current applied state and confidence for one node."""
    db = get_db()
    tags = db.execute(
        "SELECT tag_id, name, tag_type, target_entity, states_csv, display_order FROM tags"
        " WHERE deprecated_at IS NULL ORDER BY display_order, tag_id"
    ).fetchall()
    state_rows = db.execute(
        f"SELECT t1.tag_id, t1.state, t1.confidence FROM taggings t1"
        f" WHERE t1.target_type = ? AND t1.target_id = ? AND {_FOLD}",
        (target_type, target_id),
    ).fetchall()
    db.close()
    current = {str(r["tag_id"]): {"state": r["state"], "confidence": r["confidence"]} for r in state_rows}
    return jsonify({"tags": [dict(t) for t in tags], "current": current})


@bp.route("/tagging", methods=["POST"])
@login_required
def apply_tag():
    data        = request.get_json(force=True)
    tag_id      = data.get("tag_id")
    state       = data.get("state")   # None = untag event
    target_type = data.get("target_type")
    target_id   = data.get("target_id")

    if not tag_id or not target_type or not target_id:
        abort(400, "tag_id, target_type, and target_id required")

    db = get_db()
    tag = db.execute(
        "SELECT tag_id, name, tag_type, states_csv, deprecated_at FROM tags WHERE tag_id = ?",
        (tag_id,),
    ).fetchone()
    if not tag:
        db.close()
        abort(404, "tag not found")
    if tag["deprecated_at"] is not None:
        db.close()
        abort(400, "tag is deprecated")
    if tag["tag_type"] == "system":
        db.close()
        abort(400, "system tags cannot be applied manually")
    if state is not None and state not in tag["states_csv"].split(","):
        db.close()
        abort(400, f"invalid state '{state}' for tag '{tag['name']}'")

    db.execute(
        "INSERT INTO taggings (tag_id, state, target_type, target_id, user_id, system)"
        " VALUES (?, ?, ?, ?, ?, 0)",
        (tag_id, state, target_type, target_id, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})

from flask import Blueprint, jsonify, request, abort
from flask_login import login_required, current_user
from .models import get_db

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
    """All non-deprecated tags for the user-facing picker."""
    db = get_db()
    rows = db.execute(
        "SELECT tag_id, name, states_csv, display_order"
        " FROM tags WHERE deprecated_at IS NULL"
        " ORDER BY display_order, tag_id"
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/tagging/<target_type>/<path:target_id>")
def tagging_for_target(target_type, target_id):
    """Non-deprecated tags plus current applied state for one node."""
    db = get_db()
    tags = db.execute(
        "SELECT tag_id, name, states_csv, display_order FROM tags"
        " WHERE deprecated_at IS NULL ORDER BY display_order, tag_id"
    ).fetchall()
    state_rows = db.execute(
        f"SELECT t1.tag_id, t1.state FROM taggings t1"
        f" WHERE t1.target_type = ? AND t1.target_id = ? AND {_FOLD}",
        (target_type, target_id),
    ).fetchall()
    db.close()
    current = {str(r["tag_id"]): r["state"] for r in state_rows}
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
        "SELECT tag_id, name, states_csv, deprecated_at FROM tags WHERE tag_id = ?",
        (tag_id,),
    ).fetchone()
    if not tag:
        db.close()
        abort(404, "tag not found")
    if tag["deprecated_at"] is not None:
        db.close()
        abort(400, "tag is deprecated")
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

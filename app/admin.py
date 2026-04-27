from flask import Blueprint, jsonify, request
from flask_login import current_user, login_required
from werkzeug.security import generate_password_hash
from .models import get_db

bp = Blueprint("admin", __name__, url_prefix="/api/admin")

VALID_ROLES = {"guest", "user", "admin"}


def _is_admin():
    return current_user.is_authenticated and current_user.role == "admin"


def _can_change_password(target_user_id):
    if not current_user.is_authenticated:
        return False
    if _is_admin():
        return True
    return current_user.role == "user" and current_user.id == target_user_id


@bp.route("/users")
def list_users():
    db = get_db()
    rows = db.execute(
        "SELECT id, username, role, last_login FROM users ORDER BY username"
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/users", methods=["POST"])
def add_user():
    if not _is_admin():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "user"
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if role not in VALID_ROLES:
        return jsonify({"error": "Invalid role"}), 400
    db = get_db()
    try:
        db.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
            (username, generate_password_hash(password), role),
        )
        db.commit()
    except Exception:
        db.close()
        return jsonify({"error": "Username already taken"}), 409
    db.close()
    return jsonify({"ok": True}), 201


@bp.route("/usage")
def usage_log():
    db = get_db()
    rows = db.execute(
        "SELECT seq, ts, username, session_id, event_type, api_call, details, ip, user_agent"
        " FROM usage_log ORDER BY seq DESC LIMIT 2000"
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/users/<int:user_id>/password", methods=["POST"])
def change_password(user_id):
    if not _can_change_password(user_id):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    new_password = data.get("password") or ""
    if not new_password:
        return jsonify({"error": "Password required"}), 400
    db = get_db()
    db.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (generate_password_hash(new_password), user_id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})


# ── Tags ──────────────────────────────────────────────────────────────────────

_FOLD = """
    t1.event_id = (
        SELECT MAX(t2.event_id) FROM taggings t2
        WHERE t2.tag_id      = t1.tag_id
          AND t2.target_type = t1.target_type
          AND t2.target_id   = t1.target_id
    )
"""


def _tag_usage(db):
    """Returns {tag_id: {state: count}} for currently-applied taggings."""
    rows = db.execute(
        f"SELECT t1.tag_id, t1.state, COUNT(*) as n FROM taggings t1"
        f" WHERE t1.state IS NOT NULL AND {_FOLD}"
        f" GROUP BY t1.tag_id, t1.state"
    ).fetchall()
    usage = {}
    for r in rows:
        usage.setdefault(r["tag_id"], {})[r["state"]] = r["n"]
    return usage


def _affected_nodes(db, tag_id, state):
    """(target_type, target_id) pairs whose current state for tag_id is state."""
    return db.execute(
        f"SELECT t1.target_type, t1.target_id FROM taggings t1"
        f" WHERE t1.tag_id = ? AND t1.state = ? AND {_FOLD}",
        (tag_id, state),
    ).fetchall()


@bp.route("/tags")
def admin_list_tags():
    if not _is_admin():
        return jsonify({"error": "Forbidden"}), 403
    db = get_db()
    tags = db.execute(
        "SELECT tag_id, name, tag_type, target_entity, states_csv, display_order, deprecated_at"
        " FROM tags ORDER BY deprecated_at IS NOT NULL, display_order, tag_id"
    ).fetchall()
    usage = _tag_usage(db)
    db.close()
    result = []
    for t in tags:
        row = dict(t)
        row["usage"] = usage.get(t["tag_id"], {})
        result.append(row)
    return jsonify(result)


@bp.route("/tags", methods=["POST"])
def admin_create_tag():
    if not _is_admin():
        return jsonify({"error": "Forbidden"}), 403
    data          = request.get_json() or {}
    name          = (data.get("name") or "").strip()
    states_csv    = (data.get("states_csv") or "").strip()
    display_order = int(data.get("display_order", 0))

    if not name:
        return jsonify({"error": "name required"}), 400
    states = [s.strip() for s in states_csv.split(",") if s.strip()]
    if not states:
        return jsonify({"error": "at least one state required"}), 400
    if len(states) != len(set(states)):
        return jsonify({"error": "states must be unique"}), 400

    db = get_db()
    try:
        cur = db.execute(
            "INSERT INTO tags (name, states_csv, display_order) VALUES (?, ?, ?)",
            (name, ",".join(states), display_order),
        )
        db.commit()
        tag_id = cur.lastrowid
    except Exception:
        db.close()
        return jsonify({"error": "Tag name already taken"}), 409
    db.close()
    return jsonify({"ok": True, "tag_id": tag_id}), 201


@bp.route("/tags/<int:tag_id>", methods=["PATCH"])
def admin_update_tag(tag_id):
    if not _is_admin():
        return jsonify({"error": "Forbidden"}), 403
    data    = request.get_json() or {}
    confirm = bool(data.get("confirm", False))

    db = get_db()
    tag = db.execute("SELECT * FROM tags WHERE tag_id = ?", (tag_id,)).fetchone()
    if not tag:
        db.close()
        return jsonify({"error": "not found"}), 404

    new_name          = (data.get("name") or tag["name"]).strip()
    raw_csv           = data.get("states_csv", tag["states_csv"])
    new_display_order = int(data.get("display_order", tag["display_order"]))

    new_states = [s.strip() for s in raw_csv.split(",") if s.strip()]
    if not new_states:
        db.close()
        return jsonify({"error": "at least one state required"}), 400
    if len(new_states) != len(set(new_states)):
        db.close()
        return jsonify({"error": "states must be unique"}), 400

    # Compute deprecated_at for the UPDATE
    if "deprecated" in data:
        newly_deprecating = bool(data["deprecated"]) and not tag["deprecated_at"]
        clearing          = not bool(data["deprecated"])
    else:
        newly_deprecating = False
        clearing          = False

    old_states = set(tag["states_csv"].split(","))
    removed    = old_states - set(new_states)

    if removed and not confirm:
        affected = {}
        for state in removed:
            n = db.execute(
                f"SELECT COUNT(*) FROM taggings t1"
                f" WHERE t1.tag_id = ? AND t1.state = ? AND {_FOLD}",
                (tag_id, state),
            ).fetchone()[0]
            if n > 0:
                affected[state] = n
        if affected:
            db.close()
            return jsonify({
                "needs_confirm": True,
                "removed_states": [
                    {"state": s, "n_affected": n, "default_state": new_states[0]}
                    for s, n in affected.items()
                ],
            }), 409

    try:
        for state in removed:
            for row in _affected_nodes(db, tag_id, state):
                db.execute(
                    "INSERT INTO taggings (tag_id, state, target_type, target_id, user_id, system)"
                    " VALUES (?, ?, ?, ?, ?, 1)",
                    (tag_id, new_states[0], row["target_type"], row["target_id"], current_user.id),
                )
        if newly_deprecating:
            db.execute(
                "UPDATE tags SET name=?, states_csv=?, display_order=?, deprecated_at=datetime('now')"
                " WHERE tag_id=?",
                (new_name, ",".join(new_states), new_display_order, tag_id),
            )
        elif clearing:
            db.execute(
                "UPDATE tags SET name=?, states_csv=?, display_order=?, deprecated_at=NULL"
                " WHERE tag_id=?",
                (new_name, ",".join(new_states), new_display_order, tag_id),
            )
        else:
            db.execute(
                "UPDATE tags SET name=?, states_csv=?, display_order=? WHERE tag_id=?",
                (new_name, ",".join(new_states), new_display_order, tag_id),
            )
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500
    db.close()
    return jsonify({"ok": True})


@bp.route("/tags/<int:tag_id>", methods=["DELETE"])
def admin_delete_tag(tag_id):
    if not _is_admin():
        return jsonify({"error": "Forbidden"}), 403
    data    = request.get_json() or {}
    confirm = bool(data.get("confirm", False))

    db = get_db()
    tag = db.execute("SELECT tag_id, name FROM tags WHERE tag_id = ?", (tag_id,)).fetchone()
    if not tag:
        db.close()
        return jsonify({"error": "not found"}), 404

    n = db.execute(
        f"SELECT COUNT(*) FROM taggings t1"
        f" WHERE t1.tag_id = ? AND t1.state IS NOT NULL AND {_FOLD}",
        (tag_id,),
    ).fetchone()[0]

    if n > 0 and not confirm:
        db.close()
        return jsonify({
            "needs_confirm": True,
            "n_affected": n,
            "tag_name": tag["name"],
        }), 409

    try:
        if n > 0:
            rows = db.execute(
                f"SELECT t1.target_type, t1.target_id FROM taggings t1"
                f" WHERE t1.tag_id = ? AND t1.state IS NOT NULL AND {_FOLD}",
                (tag_id,),
            ).fetchall()
            for row in rows:
                db.execute(
                    "INSERT INTO taggings (tag_id, state, target_type, target_id, user_id, system)"
                    " VALUES (?, NULL, ?, ?, ?, 1)",
                    (tag_id, row["target_type"], row["target_id"], current_user.id),
                )
        db.execute("DELETE FROM tags WHERE tag_id = ?", (tag_id,))
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500
    db.close()
    return jsonify({"ok": True})

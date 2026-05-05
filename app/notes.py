import uuid
from flask import Blueprint, jsonify, request, abort
from flask_login import login_required, current_user
from .models import get_db

bp = Blueprint("notes", __name__, url_prefix="/api")


@bp.route("/notes", methods=["POST"])
@login_required
def upsert_note():
    data        = request.get_json(force=True)
    target_type = data.get("target_type")
    target_id   = data.get("target_id")
    note_id     = data.get("note_id")
    content     = data.get("content")  # None = tombstone (delete)

    db = get_db()

    if note_id:
        original = db.execute(
            "SELECT user_id FROM notes WHERE note_id = ? ORDER BY seq LIMIT 1",
            (note_id,),
        ).fetchone()
        if original and original["user_id"] != current_user.id:
            db.close()
            abort(403, "only the original author may edit this note")
    else:
        note_id = str(uuid.uuid4())

    db.execute(
        "INSERT INTO notes (target_type, target_id, note_id, content, user_id)"
        " VALUES (?, ?, ?, ?, ?)",
        (target_type, target_id, note_id, content, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True, "note_id": note_id})


@bp.route("/notes/<target_type>/<path:target_id>")
def notes_for_target(target_type, target_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT n.note_id, n.content, u.username, n.created_at,
               (SELECT COUNT(*) FROM notes n2 WHERE n2.note_id = n.note_id) > 1 AS edited
        FROM notes n
        JOIN users u ON n.user_id = u.id
        WHERE n.target_type = ? AND n.target_id = ?
          AND n.seq IN (
            SELECT MAX(seq) FROM notes
            WHERE target_type = ? AND target_id = ?
            GROUP BY note_id
          )
        HAVING n.content IS NOT NULL
        ORDER BY n.seq
        """,
        (target_type, target_id, target_type, target_id),
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

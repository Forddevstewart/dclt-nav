import uuid
from flask import Blueprint, jsonify, request, abort
from flask_login import login_required, current_user
from .models import get_db

bp = Blueprint("adj", __name__, url_prefix="/api")

VALID_VERDICTS = {"yes", "no", "unclear"}
VALID_KEYWORDS = {
    "conservation_restriction", "article_97", "deed_restriction",
    "chapter_61", "agricultural_preservation_restriction",
    "perpetual_restriction", "ccr",
}

KW_ADJ_THRESHOLD = 0.5


# ── Adjudications ─────────────────────────────────────────────────────────────

@bp.route("/adjudications", methods=["POST"])
@login_required
def adjudicate():
    data = request.get_json(force=True)
    target_type = data.get("target_type")
    target_id   = data.get("target_id")
    keyword_id  = data.get("keyword_id")
    verdict     = data.get("verdict")

    if target_type != "document":
        abort(400, "unsupported target_type")
    if keyword_id not in VALID_KEYWORDS:
        abort(400, "unknown keyword_id")
    if verdict not in VALID_VERDICTS:
        abort(400, "verdict must be yes, no, or unclear")

    db = get_db()
    db.execute(
        "INSERT INTO adjudications (target_type, target_id, keyword_id, verdict, user_id)"
        " VALUES (?, ?, ?, ?, ?)",
        (target_type, target_id, keyword_id, verdict, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})


def _latest_fold(db, target_type, target_id):
    """Latest-wins fold: returns {keyword_id: {verdict, username, created_at}}."""
    rows = db.execute(
        """
        SELECT a.keyword_id, a.verdict, u.username, a.created_at
        FROM adjudications a
        JOIN users u ON a.user_id = u.id
        WHERE a.target_type = ? AND a.target_id = ?
          AND a.seq IN (
            SELECT MAX(seq) FROM adjudications
            WHERE target_type = ? AND target_id = ?
            GROUP BY keyword_id
          )
        """,
        (target_type, target_id, target_type, target_id),
    ).fetchall()
    return {
        r["keyword_id"]: {
            "verdict":    r["verdict"],
            "username":   r["username"],
            "created_at": r["created_at"],
        }
        for r in rows
    }


@bp.route("/adjudications/document/<book>/<page>")
def adjudications_for_doc(book, page):
    db = get_db()
    result = _latest_fold(db, "document", f"{book}/{page}")
    db.close()
    return jsonify(result)


@bp.route("/adjudications/document/<book>/<page>/<keyword_id>/history")
def adjudication_history(book, page, keyword_id):
    if keyword_id not in VALID_KEYWORDS:
        abort(404)
    db = get_db()
    rows = db.execute(
        """
        SELECT a.seq, a.verdict, u.username, a.created_at
        FROM adjudications a
        JOIN users u ON a.user_id = u.id
        WHERE a.target_type = 'document' AND a.target_id = ? AND a.keyword_id = ?
        ORDER BY a.seq DESC
        """,
        (f"{book}/{page}", keyword_id),
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/adjudications/by_keyword/<keyword_id>")
def adjudications_by_keyword(keyword_id):
    """Latest verdict per document for a keyword. Used for list filtering."""
    if keyword_id not in VALID_KEYWORDS:
        abort(404)
    db = get_db()
    rows = db.execute(
        """
        SELECT a.target_id, a.verdict
        FROM adjudications a
        WHERE a.target_type = 'document' AND a.keyword_id = ?
          AND a.seq IN (
            SELECT MAX(seq) FROM adjudications
            WHERE target_type = 'document' AND keyword_id = ?
            GROUP BY target_id
          )
        """,
        (keyword_id, keyword_id),
    ).fetchall()
    db.close()
    return jsonify({r["target_id"]: r["verdict"] for r in rows})


# ── Notes ─────────────────────────────────────────────────────────────────────

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

import uuid
from pathlib import Path

from flask import Blueprint, jsonify, request, send_file, send_from_directory, abort, current_app
from flask_login import login_required, login_user, current_user
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename

from .models import get_db
from .auth import User

bp = Blueprint("pwa", __name__)

_PWA_DIR = Path(__file__).parent.parent / "pwa"


# ── Static serving ─────────────────────────────────────────────────────────────

@bp.route("/pwa/")
@bp.route("/pwa/index.html")
def pwa_index():
    return send_from_directory(_PWA_DIR, "index.html")


@bp.route("/pwa/<path:filename>")
def pwa_static(filename):
    return send_from_directory(_PWA_DIR, filename)


# ── Auth helpers ───────────────────────────────────────────────────────────────

@bp.route("/api/pwa/me")
def pwa_me():
    if not current_user.is_authenticated:
        return jsonify({"error": "not authenticated"}), 401
    return jsonify({
        "user_id": current_user.id,
        "username": current_user.username,
        "full_name": current_user.full_name,
    })


@bp.route("/api/pwa/login", methods=["POST"])
def pwa_login():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "username and password required"}), 400

    db = get_db()
    row = db.execute(
        "SELECT id, username, password_hash, role, full_name FROM users"
        " WHERE lower(username) = lower(?)",
        (username,),
    ).fetchone()
    if row and check_password_hash(row["password_hash"], password):
        db.execute(
            "UPDATE users SET last_login = datetime('now') WHERE id = ?", (row["id"],)
        )
        db.commit()
        db.close()
        login_user(
            User(row["id"], row["username"], row["role"], row["full_name"]),
            remember=True,
        )
        return jsonify({"ok": True, "username": row["username"]})
    db.close()
    return jsonify({"error": "invalid credentials"}), 401


# ── Uploads ────────────────────────────────────────────────────────────────────

@bp.route("/api/uploads/parcel/<parcel_id>")
@login_required
def uploads_for_parcel(parcel_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT u.upload_id, u.doc_type, u.filename, u.mime_type,
               u.note_text, u.on_premises, u.created_at, usr.username
        FROM portal_uploads u
        JOIN users usr ON u.user_id = usr.id
        WHERE u.parcel_id = ?
        ORDER BY u.seq DESC
        """,
        (parcel_id,),
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/api/uploads", methods=["POST"])
@login_required
def create_upload():
    parcel_id   = request.form.get("parcel_id", "").strip()
    doc_type    = request.form.get("doc_type", "").strip()
    note_text   = request.form.get("note_text", "").strip() or None
    op_raw      = request.form.get("on_premises", "")
    on_premises = 1 if op_raw in ("1", "true", "yes") else (0 if op_raw in ("0", "false", "no") else None)

    if not parcel_id:
        abort(400, "parcel_id required")
    if doc_type not in ("photo", "note"):
        abort(400, "doc_type must be 'photo' or 'note'")
    if doc_type == "note" and not note_text:
        abort(400, "note_text required for note documents")

    upload_id = str(uuid.uuid4())
    filename  = None
    mime_type = None

    file = request.files.get("file")
    if file and file.filename:
        original = secure_filename(file.filename)
        ext = Path(original).suffix.lower()
        filename = f"{upload_id}{ext}"
        upload_dir = Path(current_app.config["UPLOAD_DIR"])
        upload_dir.mkdir(parents=True, exist_ok=True)
        file.save(str(upload_dir / filename))
        mime_type = file.content_type or None

    if doc_type == "photo" and not filename:
        abort(400, "file required for photo documents")

    db = get_db()
    db.execute(
        """
        INSERT INTO portal_uploads
            (upload_id, parcel_id, doc_type, filename, mime_type, note_text, on_premises, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (upload_id, parcel_id, doc_type, filename, mime_type, note_text, on_premises, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True, "upload_id": upload_id}), 201


@bp.route("/api/uploads/<upload_id>/file")
@login_required
def upload_file(upload_id):
    db = get_db()
    row = db.execute(
        "SELECT filename, mime_type FROM portal_uploads WHERE upload_id = ?", (upload_id,)
    ).fetchone()
    db.close()
    if not row or not row["filename"]:
        abort(404)
    file_path = Path(current_app.config["UPLOAD_DIR"]) / row["filename"]
    if not file_path.exists():
        abort(404)
    return send_file(str(file_path), mimetype=row["mime_type"] or None)

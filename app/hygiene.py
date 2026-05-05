from flask import Blueprint, jsonify, request, abort
from flask_login import login_required, current_user
from .models import get_db

bp = Blueprint("hygiene", __name__, url_prefix="/api")


@bp.route("/hygiene/links/<path:link_id>", methods=["PATCH"])
@login_required
def hygiene_update_link(link_id):
    data   = request.get_json() or {}
    status = data.get("status", "")
    if status not in ("candidate", "confirmed", "rejected"):
        return jsonify({"error": "status must be candidate, confirmed, or rejected"}), 400

    try:
        doc_id, parcel_id = link_id.rsplit("|", 1)
    except ValueError:
        return jsonify({"error": "invalid link_id"}), 400

    db = get_db()
    db.execute(
        "INSERT INTO parcel_link_adjudications (doc_id, parcel_id, status, reviewed_by)"
        " VALUES (?, ?, ?, ?)",
        (doc_id, parcel_id, status, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})


@bp.route("/hygiene/links", methods=["POST"])
@login_required
def hygiene_create_link():
    """Create a manual confirmed link (user-picked parcel not detected by OCR)."""
    data        = request.get_json() or {}
    doc_id      = (data.get("doc_id") or "").strip()
    parcel_id   = (data.get("parcel_id") or "").strip()
    source_type = (data.get("source_type") or "agendacenter").strip()

    if not doc_id or not parcel_id:
        return jsonify({"error": "doc_id and parcel_id required"}), 400

    db = get_db()
    try:
        db.execute(
            "INSERT INTO parcel_link_adjudications"
            " (doc_id, parcel_id, status, source_type, match_type, confidence, reviewed_by)"
            " VALUES (?, ?, 'user_manual', ?, 'user_manual', 1.0, ?)",
            (doc_id, parcel_id, source_type, current_user.id),
        )
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500
    db.close()
    return jsonify({"ok": True, "link_id": doc_id + "|" + parcel_id}), 201


@bp.route("/hygiene/links/<path:link_id>", methods=["DELETE"])
@login_required
def hygiene_delete_link(link_id):
    try:
        doc_id, parcel_id = link_id.rsplit("|", 1)
    except ValueError:
        return jsonify({"error": "invalid link_id"}), 400

    db = get_db()
    latest = db.execute(
        "SELECT status FROM parcel_link_adjudications"
        " WHERE doc_id = ? AND parcel_id = ? ORDER BY seq DESC LIMIT 1",
        (doc_id, parcel_id),
    ).fetchone()
    if not latest or latest["status"] == "candidate":
        db.close()
        return jsonify({"error": "not found"}), 404
    db.execute(
        "INSERT INTO parcel_link_adjudications (doc_id, parcel_id, status, reviewed_by)"
        " VALUES (?, ?, 'candidate', ?)",
        (doc_id, parcel_id, current_user.id),
    )
    db.commit()
    db.close()
    return jsonify({"ok": True})

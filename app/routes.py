import os
from flask import Blueprint, render_template, send_file

bp = Blueprint("routes", __name__)

_PROCESSING_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "processing")


@bp.route("/")
def index():
    return render_template("index.html")


@bp.route("/docs/navigator-conversation")
def navigator_conversation_pdf():
    path = os.path.join(_PROCESSING_DIR, "DCLT Navigator Conversation.pdf")
    return send_file(path, mimetype="application/pdf")

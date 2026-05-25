import os
from flask import Blueprint, render_template, send_file

bp = Blueprint("routes", __name__)


@bp.route("/")
def index():
    return render_template("index.html")


@bp.route("/docs/navigator-conversation")
def navigator_conversation_pdf():
    root = os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(root, "DCLT Navigator Conversation.pdf")
    return send_file(path, mimetype="application/pdf")

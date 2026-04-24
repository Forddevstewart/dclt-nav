from flask import Blueprint, jsonify
from .models import get_all_items

bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/items")
def items():
    return jsonify(get_all_items())

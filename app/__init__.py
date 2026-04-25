import sqlite3
import os
from flask import Flask


def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    app.config["DATABASE"] = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "dclt.db"
    )

    from discovery.config import get_config
    app.config["RAW_DATABASE"] = str(get_config().db_path("raw"))

    from .routes import bp as routes_bp
    from .api import bp as api_bp

    app.register_blueprint(routes_bp)
    app.register_blueprint(api_bp)

    _init_db(app)

    return app


def _init_db(app):
    seed_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "seed.sql"
    )
    db_path = app.config["DATABASE"]
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    with open(seed_path) as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()

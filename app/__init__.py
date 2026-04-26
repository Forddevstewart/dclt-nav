import sqlite3
import os
import uuid
from flask import Flask, request, session


def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    app.config["DATABASE"] = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "dclt.db"
    )
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")

    from discovery.config import get_config
    app.config["REFERENCE_DATABASE"] = str(get_config().db_path("reference"))

    from .auth import bp as auth_bp, login_manager, ensure_ford
    from .routes import bp as routes_bp
    from .api import bp as api_bp
    from .adjudications import bp as adj_bp
    from .admin import bp as admin_bp
    from .tags import bp as tags_bp
    from .exports import bp as exports_bp

    login_manager.init_app(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(routes_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(adj_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(tags_bp)
    app.register_blueprint(exports_bp)

    _SKIP_LOG = {"/api/admin/usage", "/api/items"}

    @app.before_request
    def _ensure_session():
        if "_sid" not in session:
            session["_sid"] = uuid.uuid4().hex[:12]

    @app.after_request
    def _log_api(response):
        path = request.path
        if not path.startswith("/api/") or path in _SKIP_LOG:
            return response
        from .usage import classify, log_event
        qs = request.query_string.decode() or None
        log_event(classify(path), api_call=path, details=qs)
        return response

    _init_db(app)

    from .models import run_migrations
    run_migrations(app.config["DATABASE"])
    ensure_ford(app.config["DATABASE"])

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

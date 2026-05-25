import sqlite3
import os
import uuid
from flask import Flask, request, session


def create_app(test_config=None):
    app = Flask(__name__, template_folder="templates", static_folder="static")

    civictwin_root = os.environ.get("CIVICTWIN_ROOT", "/Volumes/DigitalTwin/CivicTwin")

    app.config["DATABASE"] = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "transactions.db"
    )
    app.config["CIVICTWIN_ROOT"] = civictwin_root
    app.config["REFERENCE_DATABASE"] = os.environ.get(
        "REFERENCE_DATABASE",
        os.path.join(civictwin_root, "db", "reference.db"),
    )
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")
    app.config["UPLOAD_DIR"] = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "uploads"
    )

    if test_config is not None:
        app.config.update(test_config)

    from .auth import bp as auth_bp, login_manager, ensure_ford
    from .routes import bp as routes_bp
    from .parcels import bp as parcels_bp
    from .documents import bp as documents_bp
    from .town_docs import bp as town_docs_bp
    from .hygiene import bp as hygiene_bp
    from .meta import bp as meta_bp
    from .admin import bp as admin_bp
    from .tags import bp as tags_bp
    from .exports import bp as exports_bp
    from .notes import bp as notes_bp
    from .pwa import bp as pwa_bp
    from .campaigns import bp as campaigns_bp
    from .reports import bp as reports_bp

    login_manager.init_app(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(routes_bp)
    app.register_blueprint(parcels_bp)
    app.register_blueprint(documents_bp)
    app.register_blueprint(town_docs_bp)
    app.register_blueprint(hygiene_bp)
    app.register_blueprint(meta_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(tags_bp)
    app.register_blueprint(exports_bp)
    app.register_blueprint(notes_bp)
    app.register_blueprint(pwa_bp)
    app.register_blueprint(campaigns_bp)
    app.register_blueprint(reports_bp)

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

    from .campaigns import seed_campaigns
    import sqlite3 as _sqlite3
    _conn = _sqlite3.connect(app.config["DATABASE"])
    _conn.row_factory = _sqlite3.Row
    seed_campaigns(_conn)
    _conn.close()

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

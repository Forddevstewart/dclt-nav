import os
import sqlite3
from flask import current_app


def get_db():
    conn = sqlite3.connect(current_app.config["DATABASE"])
    conn.row_factory = sqlite3.Row
    return conn


def get_reference_db():
    conn = sqlite3.connect(current_app.config["REFERENCE_DATABASE"])
    conn.row_factory = sqlite3.Row
    return conn


def get_all_items():
    conn = get_db()
    rows = conn.execute("SELECT * FROM items ORDER BY created_at").fetchall()
    conn.close()
    return [dict(row) for row in rows]


def run_migrations(db_path: str) -> None:
    from .migrations import MIGRATIONS

    env = os.environ.get("DCLT_ENV", "dev")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    current = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
    for version, sql in MIGRATIONS:
        if version > current:
            conn.executescript(sql)
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))
            conn.commit()

    has_dev = conn.execute(
        "SELECT COUNT(*) FROM _env_sentinel WHERE env='dev'"
    ).fetchone()[0]

    if env == "production" and has_dev:
        conn.close()
        raise RuntimeError(
            "dclt.db contains a dev sentinel — refusing to start. "
            "Restore the production database."
        )

    if env != "production" and not has_dev:
        conn.execute(
            "INSERT INTO _env_sentinel (env, detail) VALUES ('dev', 'marked on first dev use')"
        )
        conn.commit()

    conn.close()

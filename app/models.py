import sqlite3
from flask import current_app


def get_db():
    conn = sqlite3.connect(current_app.config["DATABASE"])
    conn.row_factory = sqlite3.Row
    return conn


def get_raw_db():
    conn = sqlite3.connect(current_app.config["RAW_DATABASE"])
    conn.row_factory = sqlite3.Row
    return conn


def get_all_items():
    conn = get_db()
    rows = conn.execute("SELECT * FROM items ORDER BY created_at").fetchall()
    conn.close()
    return [dict(row) for row in rows]

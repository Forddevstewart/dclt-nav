#!/usr/bin/env python3
"""
One-shot script to apply migration 10 (add full_name to users) to production.

Run on the server:
    python3 /var/www/dclt-nav/scripts/migrate_full_name.py

Safe to run multiple times — exits cleanly if the column already exists.
The app startup also applies this migration automatically on next restart.
"""

import sqlite3
import sys
import os

DB_PATH = os.environ.get(
    "DCLT_DB",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "dclt.db"),
)


def main():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Check if column already exists
    cols = [row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "full_name" in cols:
        print("full_name column already present — nothing to do.")
        conn.close()
        return

    print(f"Applying migration to {DB_PATH} ...")
    conn.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
    conn.execute("UPDATE users SET full_name = username WHERE full_name IS NULL OR full_name = ''")
    conn.commit()

    # Bump schema_version so the app doesn't try to re-apply
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    if current < 10:
        conn.execute(f"PRAGMA user_version = 10")
        conn.commit()
        print(f"schema_version: {current} → 10")

    rows = conn.execute("SELECT username, full_name FROM users ORDER BY username").fetchall()
    conn.close()

    print(f"Done. {len(rows)} user(s) updated:")
    for username, full_name in rows:
        print(f"  {username!r:20s} → full_name={full_name!r}")


if __name__ == "__main__":
    main()

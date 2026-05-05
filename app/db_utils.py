"""Shared SQLite utilities for app blueprints."""

# Latest-wins fold predicate. Use in queries as:
#   WHERE t1.event_id = (SELECT MAX(t2.event_id) FROM taggings t2
#                        WHERE t2.tag_id = t1.tag_id AND ...)
FOLD = """
    t1.event_id = (
        SELECT MAX(t2.event_id) FROM taggings t2
        WHERE t2.tag_id      = t1.tag_id
          AND t2.target_type = t1.target_type
          AND t2.target_id   = t1.target_id
    )
"""


def table_exists(db, name: str) -> bool:
    return db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()[0] > 0


def column_exists(db, table: str, column: str) -> bool:
    return any(
        row[1] == column
        for row in db.execute(f"PRAGMA table_info({table})")
    )

"""
Tests for WORM table enforcement and latest-wins fold logic.

Run: python3 -m pytest -sv app/test_adjudications.py
"""
import sqlite3
import pytest
from app.migrations import MIGRATIONS


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO users (username, password_hash) VALUES ('alice','x'),('bob','x');
    """)
    for _version, sql in MIGRATIONS:
        conn.executescript(sql)
    conn.commit()
    return conn


# ── Notes WORM enforcement ────────────────────────────────────────────────────

def test_notes_no_update(db):
    db.execute(
        "INSERT INTO notes (target_type, target_id, note_id, content, user_id)"
        " VALUES ('document','100/200','n1','hello',1)"
    )
    db.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("UPDATE notes SET content='modified' WHERE seq=1")
        db.commit()


def test_notes_no_delete(db):
    db.execute(
        "INSERT INTO notes (target_type, target_id, note_id, content, user_id)"
        " VALUES ('document','100/200','n1','hello',1)"
    )
    db.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("DELETE FROM notes WHERE seq=1")
        db.commit()


def test_note_tombstone_excluded(db):
    db.execute(
        "INSERT INTO notes (target_type, target_id, note_id, content, user_id)"
        " VALUES ('document','100/200','note-1','Original',1)"
    )
    db.commit()
    db.execute(
        "INSERT INTO notes (target_type, target_id, note_id, content, user_id)"
        " VALUES ('document','100/200','note-1',NULL,1)"
    )
    db.commit()
    row = db.execute(
        """
        SELECT n.content FROM notes n
        WHERE n.target_type='document' AND n.target_id='100/200'
          AND n.seq IN (
            SELECT MAX(seq) FROM notes
            WHERE target_type='document' AND target_id='100/200'
            GROUP BY note_id
          )
        """
    ).fetchone()
    assert row["content"] is None


# ── Taggings WORM enforcement ─────────────────────────────────────────────────

def _make_tag(conn, name="target", states="identified,contacted"):
    cur = conn.execute(
        "INSERT INTO tags (name, states_csv) VALUES (?, ?)", (name, states)
    )
    conn.commit()
    return cur.lastrowid


def _tagging(conn, tag_id, state, target_id="100/200", target_type="parcel", user_id=1):
    conn.execute(
        "INSERT INTO taggings (tag_id, state, target_type, target_id, user_id)"
        " VALUES (?, ?, ?, ?, ?)",
        (tag_id, state, target_type, target_id, user_id),
    )
    conn.commit()


def _current_state(conn, tag_id, target_id="100/200", target_type="parcel"):
    row = conn.execute(
        """
        SELECT t1.state FROM taggings t1
        WHERE t1.tag_id = ? AND t1.target_type = ? AND t1.target_id = ?
          AND t1.event_id = (
            SELECT MAX(t2.event_id) FROM taggings t2
            WHERE t2.tag_id      = t1.tag_id
              AND t2.target_type = t1.target_type
              AND t2.target_id   = t1.target_id
          )
        """,
        (tag_id, target_type, target_id),
    ).fetchone()
    return row["state"] if row else None


def test_taggings_no_update(db):
    tid = _make_tag(db)
    _tagging(db, tid, "identified")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("UPDATE taggings SET state='contacted' WHERE event_id=1")
        db.commit()


def test_taggings_no_delete(db):
    tid = _make_tag(db)
    _tagging(db, tid, "identified")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("DELETE FROM taggings WHERE event_id=1")
        db.commit()


def test_tags_table_is_mutable(db):
    """tags rows can be updated (not WORM)."""
    tid = _make_tag(db, name="before")
    db.execute("UPDATE tags SET name='after' WHERE tag_id=?", (tid,))
    db.commit()
    row = db.execute("SELECT name FROM tags WHERE tag_id=?", (tid,)).fetchone()
    assert row["name"] == "after"


def test_tagging_latest_wins_fold(db):
    tid = _make_tag(db)
    _tagging(db, tid, "identified")
    _tagging(db, tid, "contacted")
    assert _current_state(db, tid) == "contacted"


def test_untag_event_resolves_to_null(db):
    tid = _make_tag(db)
    _tagging(db, tid, "identified")
    _tagging(db, tid, None)
    assert _current_state(db, tid) is None


def test_tags_independent_per_node(db):
    tid = _make_tag(db)
    _tagging(db, tid, "identified", target_id="parcel-A")
    _tagging(db, tid, "contacted",  target_id="parcel-B")
    _tagging(db, tid, None,         target_id="parcel-A")  # untag A
    assert _current_state(db, tid, "parcel-A") is None
    assert _current_state(db, tid, "parcel-B") == "contacted"


def test_multiple_tags_independent(db):
    t1 = _make_tag(db, name="status",    states="new,reviewed")
    t2 = _make_tag(db, name="ownership", states="private,public")
    _tagging(db, t1, "new")
    _tagging(db, t2, "public")
    _tagging(db, t1, "reviewed")
    assert _current_state(db, t1) == "reviewed"
    assert _current_state(db, t2) == "public"

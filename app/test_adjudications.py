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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _adj(conn, target_id, keyword_id, verdict, user_id=1):
    conn.execute(
        "INSERT INTO adjudications"
        " (target_type, target_id, keyword_id, verdict, user_id)"
        " VALUES ('document', ?, ?, ?, ?)",
        (target_id, keyword_id, verdict, user_id),
    )
    conn.commit()


def _latest_verdict(conn, target_id, keyword_id):
    row = conn.execute(
        """
        SELECT verdict FROM adjudications
        WHERE target_type='document' AND target_id=? AND keyword_id=?
          AND seq = (
            SELECT MAX(seq) FROM adjudications
            WHERE target_type='document' AND target_id=? AND keyword_id=?
          )
        """,
        (target_id, keyword_id, target_id, keyword_id),
    ).fetchone()
    return row["verdict"] if row else None


# ── Append-only enforcement ───────────────────────────────────────────────────

def test_adjudication_no_update(db):
    _adj(db, "100/200", "conservation_restriction", "yes")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("UPDATE adjudications SET verdict='no' WHERE seq=1")
        db.commit()


def test_adjudication_no_delete(db):
    _adj(db, "100/200", "conservation_restriction", "yes")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("DELETE FROM adjudications WHERE seq=1")
        db.commit()


def test_user_tags_no_update(db):
    db.execute(
        "INSERT INTO user_tags (target_type, target_id, tag, state, user_id)"
        " VALUES ('document','100/200','test','present',1)"
    )
    db.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("UPDATE user_tags SET state='absent' WHERE seq=1")
        db.commit()


def test_user_tags_no_delete(db):
    db.execute(
        "INSERT INTO user_tags (target_type, target_id, tag, state, user_id)"
        " VALUES ('document','100/200','test','present',1)"
    )
    db.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("DELETE FROM user_tags WHERE seq=1")
        db.commit()


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


# ── Latest-wins fold ──────────────────────────────────────────────────────────

def test_latest_verdict_wins(db):
    _adj(db, "100/200", "conservation_restriction", "yes",     user_id=1)
    _adj(db, "100/200", "conservation_restriction", "no",      user_id=2)
    _adj(db, "100/200", "conservation_restriction", "unclear", user_id=1)
    assert _latest_verdict(db, "100/200", "conservation_restriction") == "unclear"


def test_keywords_independent(db):
    _adj(db, "100/200", "conservation_restriction", "yes")
    _adj(db, "100/200", "article_97",               "no")
    _adj(db, "100/200", "conservation_restriction", "no")  # overrides CR
    assert _latest_verdict(db, "100/200", "conservation_restriction") == "no"
    assert _latest_verdict(db, "100/200", "article_97") == "no"


def test_history_ordering(db):
    _adj(db, "100/200", "deed_restriction", "yes")
    _adj(db, "100/200", "deed_restriction", "no")
    _adj(db, "100/200", "deed_restriction", "unclear")
    rows = db.execute(
        "SELECT verdict FROM adjudications"
        " WHERE target_id='100/200' AND keyword_id='deed_restriction'"
        " ORDER BY seq DESC"
    ).fetchall()
    assert [r["verdict"] for r in rows] == ["unclear", "no", "yes"]


def test_by_keyword_latest_wins(db):
    _adj(db, "100/200", "deed_restriction", "yes")
    _adj(db, "101/202", "deed_restriction", "no")
    _adj(db, "100/200", "deed_restriction", "no")  # overrides yes → no
    rows = db.execute(
        """
        SELECT a.target_id, a.verdict
        FROM adjudications a
        WHERE a.target_type='document' AND a.keyword_id='deed_restriction'
          AND a.seq IN (
            SELECT MAX(seq) FROM adjudications
            WHERE target_type='document' AND keyword_id='deed_restriction'
            GROUP BY target_id
          )
        """
    ).fetchall()
    result = {r["target_id"]: r["verdict"] for r in rows}
    assert result["100/200"] == "no"
    assert result["101/202"] == "no"


def test_yes_adjudication_visible_regardless_of_confidence(db):
    """
    Slider filter rule: yes-adjudicated docs always pass, even at score=0.
    This tests the logic that the UI implements: show if adj==yes OR score>=threshold.
    """
    _adj(db, "100/200", "conservation_restriction", "yes")
    _adj(db, "101/202", "conservation_restriction", "no")

    latest = db.execute(
        """
        SELECT target_id, verdict FROM adjudications
        WHERE target_type='document' AND keyword_id='conservation_restriction'
          AND seq IN (
            SELECT MAX(seq) FROM adjudications
            WHERE target_type='document' AND keyword_id='conservation_restriction'
            GROUP BY target_id
          )
        """
    ).fetchall()
    adjs = {r["target_id"]: r["verdict"] for r in latest}

    threshold = 0.5
    doc_scores = {"100/200": 0.0, "101/202": 0.6, "102/204": 0.7}

    visible = [
        tid for tid, score in doc_scores.items()
        if adjs.get(tid) == "yes" or score >= threshold
    ]
    assert "100/200" in visible   # yes-adjudicated, score 0 — still visible
    assert "101/202" in visible   # score 0.6 >= 0.5
    assert "102/204" in visible   # score 0.7 >= 0.5


# ── Note tombstone ────────────────────────────────────────────────────────────

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


# ── Tag latest-wins (legacy user_tags) ───────────────────────────────────────

def test_tag_removed_by_absent_event(db):
    db.execute(
        "INSERT INTO user_tags (target_type, target_id, tag, state, user_id)"
        " VALUES ('document','100/200','conservation','present',1)"
    )
    db.execute(
        "INSERT INTO user_tags (target_type, target_id, tag, state, user_id)"
        " VALUES ('document','100/200','conservation','absent',1)"
    )
    db.commit()
    row = db.execute(
        "SELECT state FROM user_tags"
        " WHERE target_id='100/200' AND tag='conservation'"
        " AND seq = (SELECT MAX(seq) FROM user_tags WHERE target_id='100/200' AND tag='conservation')"
    ).fetchone()
    assert row["state"] == "absent"


# ── taggings WORM enforcement ─────────────────────────────────────────────────

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

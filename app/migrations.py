"""
Forward-only migrations for dclt.db.

Each entry is (version: int, sql: str). Applied in order at startup
if the current schema_version is behind. Never modify an applied migration;
add a new one instead.
"""

MIGRATIONS = [
    (1, """
CREATE TABLE IF NOT EXISTS adjudications (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    keyword_id  TEXT    NOT NULL,
    verdict     TEXT    NOT NULL CHECK(verdict IN ('yes','no','unclear')),
    user_id     INTEGER NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_adj_target
    ON adjudications (target_type, target_id, keyword_id, seq DESC);

CREATE TRIGGER IF NOT EXISTS no_upd_adjudications
    BEFORE UPDATE ON adjudications
    BEGIN SELECT RAISE(FAIL,'adjudications is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_adjudications
    BEFORE DELETE ON adjudications
    BEGIN SELECT RAISE(FAIL,'adjudications is append-only'); END;

CREATE TABLE IF NOT EXISTS user_tags (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    tag         TEXT    NOT NULL,
    state       TEXT    NOT NULL CHECK(state IN ('present','absent')),
    user_id     INTEGER NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tags_target
    ON user_tags (target_type, target_id, seq DESC);

CREATE TRIGGER IF NOT EXISTS no_upd_user_tags
    BEFORE UPDATE ON user_tags
    BEGIN SELECT RAISE(FAIL,'user_tags is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_user_tags
    BEFORE DELETE ON user_tags
    BEGIN SELECT RAISE(FAIL,'user_tags is append-only'); END;

CREATE TABLE IF NOT EXISTS notes (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    note_id     TEXT    NOT NULL,
    content     TEXT,
    user_id     INTEGER NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_notes_target
    ON notes (target_type, target_id, seq DESC);

CREATE TRIGGER IF NOT EXISTS no_upd_notes
    BEFORE UPDATE ON notes
    BEGIN SELECT RAISE(FAIL,'notes is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_notes
    BEFORE DELETE ON notes
    BEGIN SELECT RAISE(FAIL,'notes is append-only'); END;

CREATE TABLE IF NOT EXISTS _env_sentinel (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    env    TEXT NOT NULL,
    detail TEXT,
    set_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""),
    (2, """
ALTER TABLE users ADD COLUMN last_login TEXT;
ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user';
"""),
    (3, """
CREATE TABLE IF NOT EXISTS usage_log (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT    NOT NULL DEFAULT (datetime('now')),
    user_id    INTEGER,
    username   TEXT,
    session_id TEXT,
    event_type TEXT,
    api_call   TEXT,
    details    TEXT,
    ip         TEXT,
    user_agent TEXT
);
CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_log (ts DESC);
"""),
    (4, """
CREATE TABLE IF NOT EXISTS tags (
    tag_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    states_csv    TEXT    NOT NULL,
    display_order INTEGER NOT NULL DEFAULT 0,
    deprecated_at TEXT
);

CREATE TABLE IF NOT EXISTS taggings (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id      INTEGER NOT NULL,
    state       TEXT,
    target_type TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    user_id     INTEGER NOT NULL,
    timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
    system      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_taggings_target ON taggings (target_type, target_id, tag_id, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_taggings_tag    ON taggings (tag_id);

CREATE TRIGGER IF NOT EXISTS no_upd_taggings
    BEFORE UPDATE ON taggings
    BEGIN SELECT RAISE(FAIL,'taggings is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_taggings
    BEFORE DELETE ON taggings
    BEGIN SELECT RAISE(FAIL,'taggings is append-only'); END;
"""),
]

"""
Forward-only migrations for transactions.db.

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
    (5, """
ALTER TABLE tags ADD COLUMN tag_type TEXT NOT NULL DEFAULT 'user';

ALTER TABLE taggings ADD COLUMN confidence REAL;
ALTER TABLE taggings ADD COLUMN source TEXT;

INSERT OR IGNORE INTO tags (name, tag_type, states_csv, display_order) VALUES
    ('Conservation Restriction',           'system', 'system', 100),
    ('Article 97',                         'system', 'system', 101),
    ('Deed Restriction',                   'system', 'system', 102),
    ('Chapter 61',                         'system', 'system', 103),
    ('Ag. Preservation Restriction',       'system', 'system', 104),
    ('Perpetual Restriction',              'system', 'system', 105),
    ('CC&R',                               'system', 'system', 106);
"""),
    (6, """
ALTER TABLE tags ADD COLUMN target_entity TEXT NOT NULL DEFAULT 'any';
UPDATE tags SET target_entity = 'document' WHERE tag_type = 'system';
"""),
    (7, """
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('Zone 1 WHP',       'system', 'parcel', 'system', 200),
    ('Zone 2 WHP',       'system', 'parcel', 'system', 201),
    ('Priority Habitat', 'system', 'parcel', 'system', 202),
    ('Est. Habitat',     'system', 'parcel', 'system', 203),
    ('Nat. Community',   'system', 'parcel', 'system', 204),
    ('BioMap3 VP',       'system', 'parcel', 'system', 205),
    ('BioMap3 Wetland',  'system', 'parcel', 'system', 206),
    ('BioMap3 Core',     'system', 'parcel', 'system', 207),
    ('BioMap3 CNL',      'system', 'parcel', 'system', 208),
    ('Open Space',       'system', 'parcel', 'system', 209),
    ('Wetlands',         'system', 'parcel', 'system', 210),
    ('Structures',       'system', 'parcel', 'system', 211),
    ('Soil',             'system', 'parcel', 'system', 212);
"""),
    (8, """
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('For Sale', 'system', 'parcel', 'system', 213);
"""),
    (9, """
CREATE TABLE IF NOT EXISTS parcel_links (
    link_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id       TEXT    NOT NULL,
    source_type  TEXT    NOT NULL,
    parcel_id    TEXT    NOT NULL,
    match_type   TEXT,
    match_text   TEXT,
    confidence   REAL,
    status       TEXT    NOT NULL DEFAULT 'candidate'
                     CHECK(status IN ('candidate','confirmed','rejected')),
    reviewed_by  INTEGER,
    reviewed_at  TEXT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(doc_id, parcel_id)
);
CREATE INDEX IF NOT EXISTS idx_parcel_links_doc
    ON parcel_links (doc_id);
CREATE INDEX IF NOT EXISTS idx_parcel_links_parcel
    ON parcel_links (parcel_id, status);
"""),
    (10, """
ALTER TABLE users ADD COLUMN full_name TEXT;
UPDATE users SET full_name = username WHERE full_name IS NULL OR full_name = '';
"""),
    (11, """
CREATE TABLE IF NOT EXISTS parcel_link_adjudications (
    doc_id       TEXT NOT NULL,
    parcel_id    TEXT NOT NULL,
    status       TEXT NOT NULL CHECK(status IN ('confirmed','rejected','user_manual')),
    source_type  TEXT,
    match_type   TEXT,
    confidence   REAL,
    reviewed_by  INTEGER,
    reviewed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (doc_id, parcel_id)
);
CREATE INDEX IF NOT EXISTS idx_pla_doc
    ON parcel_link_adjudications (doc_id);
CREATE INDEX IF NOT EXISTS idx_pla_parcel
    ON parcel_link_adjudications (parcel_id, status);
INSERT OR IGNORE INTO parcel_link_adjudications
    (doc_id, parcel_id, status, source_type, match_type, confidence, reviewed_by, reviewed_at)
    SELECT doc_id, parcel_id,
           CASE WHEN match_type = 'user_manual' THEN 'user_manual' ELSE status END,
           source_type, match_type, confidence, reviewed_by,
           COALESCE(reviewed_at, created_at, datetime('now'))
    FROM parcel_links
    WHERE status IN ('confirmed', 'rejected') OR match_type = 'user_manual';
DROP TABLE IF EXISTS parcel_links;
"""),
    (12, """
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('Development Status', 'user', 'parcel', 'undeveloped,underdeveloped', 300);
"""),
    (13, """
-- Deprecate all system tags (OCR keyword, GIS presence, For Sale).
-- Their attribute data lives in reference.db Layers (registry_ocr, parcels_gis,
-- layer_for_sale). Existing system=1 rows in taggings are a historical archive;
-- application code no longer reads them.
UPDATE tags SET deprecated_at = datetime('now')
    WHERE tag_type = 'system' AND deprecated_at IS NULL;

-- Deprecate Development Status (superseded by CoverageDetermination below).
-- Existing tagging events under Development Status are preserved in full history.
UPDATE tags SET deprecated_at = datetime('now')
    WHERE name = 'Development Status' AND deprecated_at IS NULL;

-- Insert CA-aligned Tag dimensions.
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('CoverageDetermination',  'user', 'parcel',   'Unconfirmed,Undeveloped,Underdeveloped,Developed', 300),
    ('IdentityResolution',     'user', 'parcel',   'Unconfirmed,ADB Add,ADB Remove,GIS Add,GIS Remove', 310),
    ('Article97Determination', 'user', 'document', 'Unconfirmed,Confirmed,Denied', 320);
"""),
    (14, """
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('FarmingDetermination', 'user', 'parcel', 'Unconfirmed,Not Suitable,Possible,Suitable', 330);
"""),
    (15, """
-- Deprecate informal 'Acquisition' tag superseded by AcquisitionDetermination.
UPDATE tags SET deprecated_at = datetime('now')
    WHERE name = 'Acquisition' AND deprecated_at IS NULL;

-- Register AcquisitionDetermination dimension.
-- Applicability: ParcelAcquisitionSuitability ∈ {Possible, Likely} (gate enforced in dimensions.py).
-- Default: Unconfirmed (applicable parcels only). Transitions: any-to-any.
INSERT OR IGNORE INTO tags (name, tag_type, target_entity, states_csv, display_order) VALUES
    ('AcquisitionDetermination', 'user', 'parcel', 'Unconfirmed,Pursue,Watch,Pass', 340);
"""),
    (16, """
-- Filter entry points: curated deep-link payloads for node-type entry points
-- (e.g. left-nav Suitability category clicks). Each entry point carries an
-- ordered list of (dimension, selection) pairs that pre-populate filter chips
-- when the user arrives via that entry point.
CREATE TABLE IF NOT EXISTS filter_entry_points (
    entry_point_id TEXT PRIMARY KEY,
    node_type      TEXT NOT NULL,
    label          TEXT NOT NULL,
    group_label    TEXT,
    payload_json   TEXT NOT NULL,
    display_order  INTEGER NOT NULL DEFAULT 0,
    created_by     INTEGER,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    deprecated_at  TEXT
);

-- Append-only audit log for entry point changes.
CREATE TABLE IF NOT EXISTS filter_entry_point_log (
    log_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_point_id TEXT NOT NULL,
    change_type    TEXT NOT NULL,
    old_payload    TEXT,
    new_payload    TEXT,
    changed_by     INTEGER,
    changed_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ParcelAcquisitionSuitability entry points.
-- Each lands on the parcel list with Suitability pre-set and
-- AcquisitionDetermination = Unconfirmed so the user enters the work queue.
INSERT OR IGNORE INTO filter_entry_points
    (entry_point_id, node_type, label, group_label, payload_json, display_order) VALUES
    ('parcel.acq_suitability.likely',
     'parcel', 'Likely', 'Acquisition Suitability',
     '[{"dimension":"ParcelAcquisitionSuitability","selection":"Likely"},{"dimension":"AcquisitionDetermination","selection":"Unconfirmed"}]',
     10),
    ('parcel.acq_suitability.possible',
     'parcel', 'Possible', 'Acquisition Suitability',
     '[{"dimension":"ParcelAcquisitionSuitability","selection":"Possible"},{"dimension":"AcquisitionDetermination","selection":"Unconfirmed"}]',
     20);
"""),
    (17, """
-- Rebuild parcel_link_adjudications as append-only (seq PK, WORM triggers).
-- Existing rows are preserved as the initial history fold; latest seq per
-- (doc_id, parcel_id) is the current adjudication. 'candidate' added to the
-- status space so a revert-to-unreviewed can be expressed as a new event.
ALTER TABLE parcel_link_adjudications RENAME TO _pla_old;
CREATE TABLE parcel_link_adjudications (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id       TEXT    NOT NULL,
    parcel_id    TEXT    NOT NULL,
    status       TEXT    NOT NULL CHECK(status IN ('confirmed','rejected','user_manual','candidate')),
    source_type  TEXT,
    match_type   TEXT,
    confidence   REAL,
    reviewed_by  INTEGER,
    reviewed_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO parcel_link_adjudications
    (doc_id, parcel_id, status, source_type, match_type, confidence, reviewed_by, reviewed_at)
SELECT doc_id, parcel_id, status, source_type, match_type, confidence, reviewed_by, reviewed_at
FROM _pla_old;
DROP TABLE _pla_old;
CREATE INDEX IF NOT EXISTS idx_pla_doc
    ON parcel_link_adjudications (doc_id);
CREATE INDEX IF NOT EXISTS idx_pla_parcel
    ON parcel_link_adjudications (parcel_id, status);
CREATE INDEX IF NOT EXISTS idx_pla_fold
    ON parcel_link_adjudications (doc_id, parcel_id, seq DESC);
CREATE TRIGGER IF NOT EXISTS no_upd_parcel_link_adjudications
    BEFORE UPDATE ON parcel_link_adjudications
    BEGIN SELECT RAISE(FAIL,'parcel_link_adjudications is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_parcel_link_adjudications
    BEFORE DELETE ON parcel_link_adjudications
    BEGIN SELECT RAISE(FAIL,'parcel_link_adjudications is append-only'); END;
"""),
    (18, """
-- Drop pre-dimension keyword adjudication and user_tags tables.
-- These were superseded by the Tag/Dimension system (migration 13+).
-- Data was never in active use; rows are discarded intentionally.
DROP TABLE IF EXISTS adjudications;
DROP TABLE IF EXISTS user_tags;
"""),
    (19, """
-- Portal-uploaded documents (photos and text notes) captured from the field PWA.
-- Binaries live on disk at UPLOAD_DIR/{filename}; this table holds metadata only.
CREATE TABLE IF NOT EXISTS portal_uploads (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id    TEXT    NOT NULL UNIQUE,
    parcel_id    TEXT    NOT NULL,
    doc_type     TEXT    NOT NULL CHECK(doc_type IN ('photo','note')),
    filename     TEXT,
    mime_type    TEXT,
    note_text    TEXT,
    user_id      INTEGER NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_portal_uploads_parcel
    ON portal_uploads (parcel_id, seq DESC);
CREATE TRIGGER IF NOT EXISTS no_upd_portal_uploads
    BEFORE UPDATE ON portal_uploads
    BEGIN SELECT RAISE(FAIL,'portal_uploads is append-only'); END;
CREATE TRIGGER IF NOT EXISTS no_del_portal_uploads
    BEFORE DELETE ON portal_uploads
    BEGIN SELECT RAISE(FAIL,'portal_uploads is append-only'); END;
"""),
]

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    description TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO items (id, name, description) VALUES
    (1, 'Dennis Town Hall',    'Municipal building and administrative offices'),
    (2, 'Dennis Public Library', 'Branch library serving the Dennis community'),
    (3, 'Cape Cod Rail Trail', 'Multi-use trail running through Dennis');

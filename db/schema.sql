CREATE TABLE IF NOT EXISTS content (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tmdb_id         INTEGER UNIQUE NOT NULL,
    imdb_id         TEXT,
    title           TEXT NOT NULL,
    content_type    TEXT NOT NULL CHECK(content_type IN ('movie','series')),
    release_year    INTEGER NOT NULL,
    imdb_rating     REAL,
    genres          TEXT NOT NULL DEFAULT '[]',
    poster_url      TEXT,
    plot            TEXT,
    director        TEXT,
    seret_id        INTEGER,
    seret_rating    REAL,
    seret_votes     INTEGER,
    title_he        TEXT DEFAULT '',
    plot_he         TEXT DEFAULT '',
    genres_he       TEXT DEFAULT '[]',
    last_refreshed  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profiles (
    id      INTEGER PRIMARY KEY,
    name    TEXT NOT NULL
);

INSERT OR IGNORE INTO profiles VALUES (1, 'User 1');
INSERT OR IGNORE INTO profiles VALUES (2, 'User 2');
INSERT OR IGNORE INTO profiles VALUES (3, 'User 3');
INSERT OR IGNORE INTO profiles VALUES (4, 'User 4');
INSERT OR IGNORE INTO profiles VALUES (5, 'User 5');
INSERT OR IGNORE INTO profiles VALUES (6, 'User 6');
INSERT OR IGNORE INTO profiles VALUES (7, 'User 7');
INSERT OR IGNORE INTO profiles VALUES (8, 'User 8');

CREATE TABLE IF NOT EXISTS ratings (
    profile_id  INTEGER NOT NULL REFERENCES profiles(id),
    content_id  INTEGER NOT NULL REFERENCES content(id),
    rating      REAL NOT NULL CHECK(rating BETWEEN 1 AND 10),
    rated_at    TEXT NOT NULL,
    PRIMARY KEY (profile_id, content_id)
);

CREATE TABLE IF NOT EXISTS genre_affinity (
    profile_id  INTEGER NOT NULL REFERENCES profiles(id),
    genre       TEXT NOT NULL,
    score       REAL NOT NULL,
    sample_size INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (profile_id, genre)
);

CREATE TABLE IF NOT EXISTS refresh_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    titles_added    INTEGER DEFAULT 0,
    titles_removed  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running'
);

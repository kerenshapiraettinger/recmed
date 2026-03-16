import os
import sqlite3
from config import DB_PATH

DATABASE_URL = os.environ.get("DATABASE_URL")


def _adapt(sql):
    """Convert SQLite ? placeholders to PostgreSQL %s."""
    if DATABASE_URL:
        return sql.replace("?", "%s")
    return sql


def _pg_conn():
    import psycopg2
    import psycopg2.extras
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def _sqlite_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_connection():
    return _pg_conn() if DATABASE_URL else _sqlite_conn()


def init_db():
    import config
    schema_file = "schema_pg.sql" if DATABASE_URL else "schema.sql"
    schema_path = os.path.join(os.path.dirname(__file__), schema_file)
    with open(schema_path) as f:
        schema = f.read()

    if DATABASE_URL:
        conn = _pg_conn()
        try:
            cur = conn.cursor()
            for stmt in [s.strip() for s in schema.split(";") if s.strip()]:
                cur.execute(stmt)
            conn.commit()
            # Migrate rating column to REAL if it's still INTEGER
            try:
                cur.execute("ALTER TABLE ratings ALTER COLUMN rating TYPE REAL USING rating::real")
                conn.commit()
            except Exception:
                conn.rollback()
            # Add Hebrew columns if missing
            for col, default in [("title_he", "''"), ("plot_he", "''"), ("genres_he", "'[]'")]:
                try:
                    cur.execute(f"ALTER TABLE content ADD COLUMN IF NOT EXISTS {col} TEXT DEFAULT {default}")
                    conn.commit()
                except Exception:
                    conn.rollback()
            # Add avatar column if missing
            try:
                cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS avatar TEXT DEFAULT '🎬'")
                conn.commit()
            except Exception:
                conn.rollback()
            for pid, name in config.PROFILES.items():
                cur.execute("UPDATE profiles SET name = %s WHERE id = %s", (name, pid))
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _sqlite_conn()
        conn.executescript(schema)
        for col, default in [("title_he", "''"), ("plot_he", "''"), ("genres_he", "'[]'")]:
            try:
                conn.execute(f"ALTER TABLE content ADD COLUMN {col} TEXT DEFAULT {default}")
                conn.commit()
            except Exception:
                pass
        try:
            conn.execute("ALTER TABLE profiles ADD COLUMN avatar TEXT DEFAULT '🎬'")
            conn.commit()
        except Exception:
            pass
        for pid, name in config.PROFILES.items():
            conn.execute("UPDATE profiles SET name = ? WHERE id = ?", (pid, name))
        conn.commit()
        conn.close()


def query(sql, params=(), one=False):
    sql = _adapt(sql)
    conn = get_connection()
    try:
        if DATABASE_URL:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        else:
            cur = conn.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return (rows[0] if rows else None) if one else rows


def execute(sql, params=()):
    sql_orig = sql
    sql = _adapt(sql)
    conn = get_connection()
    try:
        if DATABASE_URL:
            cur = conn.cursor()
            cur.execute(sql, params)
            last_id = None
            if sql_orig.strip().upper().startswith("INSERT"):
                cur.execute("SAVEPOINT _lastval")
                try:
                    cur.execute("SELECT lastval()")
                    row = cur.fetchone()
                    last_id = int(row["lastval"]) if row else None
                    cur.execute("RELEASE SAVEPOINT _lastval")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT _lastval")
            conn.commit()
            return last_id
        else:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


def execute_rowcount(sql, params=()):
    """Execute SQL and return the number of affected rows."""
    sql = _adapt(sql)
    conn = get_connection()
    try:
        if DATABASE_URL:
            cur = conn.cursor()
            cur.execute(sql, params)
            rowcount = cur.rowcount
            conn.commit()
            return rowcount
        else:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def executemany(sql, param_list):
    sql = _adapt(sql)
    conn = get_connection()
    try:
        if DATABASE_URL:
            cur = conn.cursor()
            cur.executemany(sql, param_list)
            conn.commit()
        else:
            conn.executemany(sql, param_list)
            conn.commit()
    finally:
        conn.close()

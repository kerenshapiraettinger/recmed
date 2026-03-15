import sqlite3
import os
from config import DB_PATH

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    import config
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        schema = f.read()
    with get_connection() as conn:
        conn.executescript(schema)
        # Sync profile names from env vars so they survive server restarts
        for pid, name in config.PROFILES.items():
            conn.execute("UPDATE profiles SET name = ? WHERE id = ?", (pid, name))
        conn.commit()

def query(sql, params=(), one=False):
    with get_connection() as conn:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    return (rows[0] if rows else None) if one else rows

def execute(sql, params=()):
    with get_connection() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid

def executemany(sql, param_list):
    with get_connection() as conn:
        conn.executemany(sql, param_list)
        conn.commit()

import json
from datetime import datetime, date
from db.database import query, execute, execute_rowcount
from ingestion.tmdb_client import discover, get_genre_map
import config


def run_refresh():
    started = datetime.utcnow().isoformat()
    log_id = execute(
        "INSERT INTO refresh_log (started_at, status) VALUES (?, 'running')",
        (started,)
    )

    added = 0

    try:
        movie_genres = get_genre_map("movie")
        tv_genres = get_genre_map("tv")

        rows = []
        for item in discover("movie", movie_genres):
            rows.append(item)
        for item in discover("tv", tv_genres):
            rows.append(item)

        for item in rows:
            existing = query(
                "SELECT id FROM content WHERE tmdb_id = ?", (item["tmdb_id"],), one=True
            )
            if existing:
                execute(
                    """UPDATE content SET title=?, imdb_rating=?, genres=?,
                       poster_url=?, last_refreshed=? WHERE tmdb_id=?""",
                    (item["title"], item["imdb_rating"], item["genres"],
                     item["poster_url"], item["last_refreshed"], item["tmdb_id"])
                )
            else:
                execute(
                    """INSERT INTO content
                       (tmdb_id, title, content_type, release_year, imdb_rating,
                        genres, poster_url, last_refreshed)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (item["tmdb_id"], item["title"], item["content_type"],
                     item["release_year"], item["imdb_rating"], item["genres"],
                     item["poster_url"], item["last_refreshed"])
                )
                added += 1

        cutoff = date.today().year - config.CONTENT_YEARS
        removed = execute_rowcount(
            """DELETE FROM content WHERE release_year < ?
               AND id NOT IN (SELECT DISTINCT content_id FROM ratings)""",
            (cutoff,)
        )

        execute(
            """UPDATE refresh_log SET finished_at=?, titles_added=?,
               titles_removed=?, status='ok' WHERE id=?""",
            (datetime.utcnow().isoformat(), added, removed, log_id)
        )
        print(f"[refresh] Done — added {added}, removed {removed}")

    except Exception as e:
        execute(
            "UPDATE refresh_log SET finished_at=?, status='error' WHERE id=?",
            (datetime.utcnow().isoformat(), log_id)
        )
        print(f"[refresh] Error: {e}")
        raise

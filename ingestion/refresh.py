import json
import time
from datetime import datetime, date
from db.database import query, execute, execute_rowcount
from ingestion.tmdb_client import discover, get_genre_map, get_watch_providers
import config


def run_refresh():
    started = datetime.utcnow().isoformat()
    log_id = execute(
        "INSERT INTO refresh_log (started_at, status) VALUES (?, 'running')",
        (started,)
    )

    added = 0

    try:
        # Fetch genre maps in both languages
        movie_genres_en = get_genre_map("movie", "en-US")
        tv_genres_en    = get_genre_map("tv",    "en-US")
        movie_genres_he = get_genre_map("movie", "he-IL")
        tv_genres_he    = get_genre_map("tv",    "he-IL")

        # Fetch all titles in English, indexed by tmdb_id
        en_items = {}
        for item in discover("movie", movie_genres_en, "en-US"):
            en_items[item["tmdb_id"]] = item
        for item in discover("tv", tv_genres_en, "en-US"):
            en_items[item["tmdb_id"]] = item

        # Fetch Hebrew data for the same titles
        he_items = {}
        for item in discover("movie", movie_genres_he, "he-IL"):
            he_items[item["tmdb_id"]] = item
        for item in discover("tv", tv_genres_he, "he-IL"):
            he_items[item["tmdb_id"]] = item

        # Merge: attach Hebrew fields onto English items
        rows = []
        for tmdb_id, item in en_items.items():
            he = he_items.get(tmdb_id, {})
            item["title_he"]  = he.get("title", "") or ""
            item["plot_he"]   = he.get("plot", "") or ""
            item["genres_he"] = he.get("genres", "[]")
            rows.append(item)

        for item in rows:
            existing = query(
                "SELECT id, plot FROM content WHERE tmdb_id = ?", (item["tmdb_id"],), one=True
            )
            if existing:
                saved_plot = existing["plot"] or item.get("plot") or ""
                execute(
                    """UPDATE content SET title=?, imdb_rating=?, genres=?,
                       poster_url=?, plot=?,
                       title_he=?, plot_he=?, genres_he=?,
                       last_refreshed=?
                       WHERE tmdb_id=?""",
                    (item["title"], item["imdb_rating"], item["genres"],
                     item["poster_url"], saved_plot,
                     item["title_he"], item["plot_he"], item["genres_he"],
                     item["last_refreshed"], item["tmdb_id"])
                )
            else:
                execute(
                    """INSERT INTO content
                       (tmdb_id, title, content_type, release_year, imdb_rating,
                        genres, poster_url, plot,
                        title_he, plot_he, genres_he,
                        last_refreshed)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item["tmdb_id"], item["title"], item["content_type"],
                     item["release_year"], item["imdb_rating"], item["genres"],
                     item["poster_url"], item.get("plot", ""),
                     item["title_he"], item["plot_he"], item["genres_he"],
                     item["last_refreshed"])
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


def run_backfill_he():
    """Fetch Hebrew title/plot and streaming for titles that are missing them."""
    from ingestion.tmdb_client import get_title_he, get_watch_providers
    rows = query(
        "SELECT id, tmdb_id, content_type, title_he FROM content WHERE title_he IS NULL OR title_he = ''"
    )
    print(f"[backfill] {len(rows)} titles missing Hebrew data")
    for row in rows:
        try:
            ctype_api = "movie" if row["content_type"] == "movie" else "tv"
            title_he, plot_he = get_title_he(row["tmdb_id"], ctype_api)
            providers = get_watch_providers(row["tmdb_id"], ctype_api)
            execute(
                "UPDATE content SET title_he=?, plot_he=?, streaming=? WHERE id=?",
                (title_he, plot_he, json.dumps(providers), row["id"])
            )
        except Exception as e:
            print(f"[backfill] Error for id {row['id']}: {e}")
        time.sleep(0.25)
    print("[backfill] Done")


def run_streaming_refresh():
    """Slowly update streaming availability for all titles. Runs after main refresh."""
    from ingestion.kan11_client import match_kan11

    print("[streaming] Starting streaming provider update...")
    titles = query("SELECT id, tmdb_id, content_type, title_he FROM content ORDER BY id")

    # Fetch Kan 11 matches up front (one page scrape)
    kan11_ids = match_kan11([(r["id"], r["title_he"]) for r in titles])

    updated = 0
    for row in titles:
        try:
            ctype_api = "movie" if row["content_type"] == "movie" else "tv"
            providers = get_watch_providers(row["tmdb_id"], ctype_api)
            if row["id"] in kan11_ids and "Kan 11" not in providers:
                providers.append("Kan 11")
            execute(
                "UPDATE content SET streaming=? WHERE id=?",
                (json.dumps(providers), row["id"])
            )
            updated += 1
        except Exception as e:
            print(f"[streaming] Error for id {row['id']}: {e}")
        time.sleep(0.25)
    print(f"[streaming] Done — updated {updated} titles")

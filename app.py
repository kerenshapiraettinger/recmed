import os
import threading
import time
import json
from datetime import datetime
from flask import (Flask, render_template, session, redirect,
                   url_for, request, jsonify, abort)

import config
from db.database import init_db, query, execute
from recommender.engine import (rebuild_affinity, get_recommendations,
                                get_genre_insights, get_watched)
from ingestion.tmdb_client import search_tmdb, fetch_imdb_id
from ingestion.omdb_client import fetch_plot
from ingestion.refresh import run_refresh
from translations import TRANSLATIONS

app = Flask(__name__)
app.secret_key = config.SECRET_KEY


@app.context_processor
def inject_lang():
    lang = session.get('lang', 'en')
    return {'t': TRANSLATIONS[lang], 'lang': lang}


# ── helpers ───────────────────────────────────────────────────────────────────

def current_profile():
    pid = session.get("profile_id")
    if pid not in config.PROFILES:
        return None
    row = query("SELECT name FROM profiles WHERE id = ?", (pid,), one=True)
    name = row["name"] if row else config.PROFILES[pid]
    return {"id": pid, "name": name}

def require_profile():
    p = current_profile()
    if not p:
        abort(redirect(url_for("index")))
    return p


# ── routes ────────────────────────────────────────────────────────────────────

def get_all_profiles():
    rows = query("SELECT id, name FROM profiles ORDER BY id")
    return {row["id"]: row["name"] for row in rows}

@app.route("/")
def index():
    profile = current_profile()
    return render_template("index.html", profiles=get_all_profiles(), profile=profile)

@app.route("/set_lang/<lang>")
def set_lang(lang):
    if lang in ('en', 'he'):
        session['lang'] = lang
    return redirect(request.referrer or url_for('index'))


@app.route("/profile/<int:pid>/rename", methods=["POST"])
def rename_profile(pid):
    if pid not in config.PROFILES:
        abort(404)
    name = request.form.get("name", "").strip()
    if name:
        execute("UPDATE profiles SET name = ? WHERE id = ?", (name, pid))
        if session.get("profile_id") == pid:
            pass  # current_profile() will re-read from DB on next request
    return redirect(url_for("index"))


@app.route("/profile/<int:pid>")
def set_profile(pid):
    if pid not in config.PROFILES:
        return redirect(url_for("index"))
    session["profile_id"] = pid
    return redirect(url_for("recommendations"))


@app.route("/recommendations")
def recommendations():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    genre_filter = request.args.get("genre", "").strip()

    all_recs = get_recommendations(profile["id"], limit=100)
    insights = get_genre_insights(profile["id"])
    for item in all_recs:
        try:
            item["genres_list"] = json.loads(item["genres"])
        except Exception:
            item["genres_list"] = []

    # Collect genres that actually appear in recs (preserving ranking order)
    seen_genres = set()
    all_genres = []
    for item in all_recs:
        for g in item["genres_list"]:
            if g not in seen_genres:
                all_genres.append(g)
                seen_genres.add(g)

    if genre_filter:
        grouped = {genre_filter: [i for i in all_recs if genre_filter in i["genres_list"]]}
    else:
        grouped = {}
        for item in all_recs:
            primary = item["genres_list"][0] if item["genres_list"] else "Other"
            grouped.setdefault(primary, []).append(item)

    return render_template("recommendations.html",
                           profile=profile, grouped=grouped,
                           all_genres=all_genres, genre_filter=genre_filter,
                           insights=insights)


@app.route("/watched")
def watched():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))
    items = get_watched(profile["id"])
    for item in items:
        try:
            item["genres_list"] = json.loads(item["genres"])
        except Exception:
            item["genres_list"] = []
    return render_template("watched.html", profile=profile, items=items)


@app.route("/browse")
def browse():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    genre_filter = request.args.get("genre", "")
    type_filter  = request.args.get("type", "")
    year_filter  = request.args.get("year", "")

    sql = "SELECT * FROM content WHERE 1=1"
    params = []
    if genre_filter:
        sql += " AND genres LIKE ?"
        params.append(f"%{genre_filter}%")
    if type_filter:
        sql += " AND content_type = ?"
        params.append(type_filter)
    if year_filter:
        sql += " AND release_year = ?"
        params.append(int(year_filter))
    sql += " ORDER BY imdb_rating DESC LIMIT 100"

    items = query(sql, params)
    items = [dict(i) for i in items]
    for item in items:
        try:
            item["genres_list"] = json.loads(item["genres"])
        except Exception:
            item["genres_list"] = []

    # All unique genres for filter dropdown
    all_genres = set()
    for row in query("SELECT genres FROM content"):
        try:
            for g in json.loads(row["genres"]):
                all_genres.add(g)
        except Exception:
            pass

    return render_template("browse.html", profile=profile, items=items,
                           all_genres=sorted(all_genres),
                           genre_filter=genre_filter,
                           type_filter=type_filter,
                           year_filter=year_filter)


@app.route("/title/<int:content_id>")
def title_detail(content_id):
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    item = query("SELECT * FROM content WHERE id = ?", (content_id,), one=True)
    if not item:
        abort(404)
    item = dict(item)

    # Lazy OMDb enrichment
    if not item.get("plot") and config.OMDB_API_KEY:
        imdb_id = item.get("imdb_id")
        if not imdb_id:
            imdb_id = fetch_imdb_id(
                item["tmdb_id"],
                "movie" if item["content_type"] == "movie" else "tv"
            )
            if imdb_id:
                execute("UPDATE content SET imdb_id = ? WHERE id = ?",
                        (imdb_id, content_id))
        if imdb_id:
            plot = fetch_plot(imdb_id)
            if plot:
                execute("UPDATE content SET plot = ? WHERE id = ?",
                        (plot, content_id))
                item["plot"] = plot

    try:
        item["genres_list"] = json.loads(item["genres"])
    except Exception:
        item["genres_list"] = []

    user_rating = query(
        "SELECT rating FROM ratings WHERE profile_id = ? AND content_id = ?",
        (profile["id"], content_id), one=True
    )
    item["user_rating"] = user_rating["rating"] if user_rating else None

    return render_template("detail.html", profile=profile, item=item)


@app.route("/search")
def search():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))
    q = request.args.get("q", "").strip()
    results = []
    if q:
        # First search local DB
        db_results = query(
            "SELECT * FROM content WHERE title LIKE ? ORDER BY imdb_rating DESC LIMIT 20",
            (f"%{q}%",)
        )
        if db_results:
            results = [dict(r) for r in db_results]
            for item in results:
                item["source"] = "db"
                try:
                    item["genres_list"] = json.loads(item["genres"])
                except Exception:
                    item["genres_list"] = []
        else:
            # Fall back to TMDB search
            tmdb_results = search_tmdb(q, language=session.get('lang', 'en'))
            for item in tmdb_results:
                item["source"] = "tmdb"
                item["id"] = None
                try:
                    item["genres_list"] = json.loads(item["genres"])
                except Exception:
                    item["genres_list"] = []
            results = tmdb_results

    return render_template("search.html", profile=profile, results=results, q=q)


@app.route("/add_from_search", methods=["POST"])
def add_from_search():
    """Add a TMDB result (not yet in DB) and redirect to its detail page."""
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    tmdb_id    = int(request.form["tmdb_id"])
    title      = request.form["title"]
    ctype      = request.form["content_type"]
    year       = int(request.form.get("release_year", 0))
    rating_val = float(request.form.get("imdb_rating", 0) or 0)
    poster     = request.form.get("poster_url", "")
    plot       = request.form.get("plot", "")

    today = datetime.utcnow().date().isoformat()

    # Upsert into content
    existing = query("SELECT id FROM content WHERE tmdb_id = ?", (tmdb_id,), one=True)
    if existing:
        content_id = existing["id"]
        if plot:
            execute("UPDATE content SET plot = ? WHERE id = ? AND (plot IS NULL OR plot = '')",
                    (plot, content_id))
    else:
        content_id = execute(
            """INSERT INTO content (tmdb_id, title, content_type, release_year,
               imdb_rating, genres, poster_url, plot, last_refreshed)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (tmdb_id, title, ctype, year, rating_val, "[]", poster, plot, today)
        )

    return redirect(url_for("title_detail", content_id=content_id))


@app.route("/rate", methods=["POST"])
def rate():
    profile = current_profile()
    if not profile:
        return jsonify({"error": "no profile"}), 401

    data = request.get_json()
    content_id  = int(data["content_id"])
    user_rating = float(data["rating"])

    if not (1 <= user_rating <= 10):
        return jsonify({"error": "rating must be 1-10"}), 400

    execute(
        """INSERT INTO ratings (profile_id, content_id, rating, rated_at)
           VALUES (?,?,?,?)
           ON CONFLICT(profile_id, content_id) DO UPDATE SET rating=excluded.rating""",
        (profile["id"], content_id, user_rating, datetime.utcnow().isoformat())
    )
    rebuild_affinity(profile["id"])
    return jsonify({"ok": True})


@app.route("/admin/refresh")
def admin_refresh():
    secret = request.args.get("secret", "")
    if secret != config.ADMIN_SECRET:
        abort(403)
    t = threading.Thread(target=run_refresh, daemon=True)
    t.start()
    return "Refresh started in background. Check <a href='/admin/status?secret=" + config.ADMIN_SECRET + "'>status</a> for progress."


@app.route("/admin/status")
def admin_status():
    secret = request.args.get("secret", "")
    if secret != config.ADMIN_SECRET:
        abort(403)
    logs = query("SELECT * FROM refresh_log ORDER BY id DESC LIMIT 10")
    total = query("SELECT COUNT(*) as c FROM content", one=True)["c"]
    return render_template("admin_status.html", logs=logs, total=total,
                           admin_secret=config.ADMIN_SECRET)


# ── background refresh ────────────────────────────────────────────────────────

def _refresh_loop():
    # Wait 5 seconds after startup, then run immediately if DB is empty
    time.sleep(5)
    count = query("SELECT COUNT(*) as c FROM content", one=True)
    if count and count["c"] == 0:
        print("[refresh] DB empty — running initial refresh...")
        try:
            run_refresh()
        except Exception as e:
            print(f"[refresh] Initial refresh failed: {e}")

    while True:
        time.sleep(30 * 24 * 3600)  # 30 days
        try:
            run_refresh()
        except Exception as e:
            print(f"[refresh] Scheduled refresh failed: {e}")


# ── startup ───────────────────────────────────────────────────────────────────

# Runs on every startup (python app.py AND gunicorn)
init_db()
t = threading.Thread(target=_refresh_loop, daemon=True)
t.start()

if __name__ == "__main__":
    app.run(debug=False, port=5000)

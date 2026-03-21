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
from ingestion.refresh import run_refresh, run_streaming_refresh
from translations import TRANSLATIONS

AVATARS = [
    # Male
    '👨','👦','🧔','👴',
    # Female
    '👩','👧','👱‍♀️','👵',
    # Neutral / fun
    '🧑','🧒','🤖','🧙',
    # Animals
    '🐱','🐶','🦊','🐻',
    # Movie / objects
    '🎬','🎥','🍿','🎭',
    # Nature / landscape
    '🌙','⭐','🏔️','🌊',
]

app = Flask(__name__)
app.secret_key = config.SECRET_KEY


@app.context_processor
def inject_lang():
    lang = session.get('lang', 'en')
    last_row = query(
        "SELECT finished_at FROM refresh_log WHERE status='ok' ORDER BY id DESC LIMIT 1",
        one=True
    )
    last_updated = None
    if last_row and last_row["finished_at"]:
        try:
            dt = datetime.fromisoformat(last_row["finished_at"])
            last_updated = dt.strftime("%-d/%-m/%Y")
        except Exception:
            last_updated = last_row["finished_at"][:10]
    return {'t': TRANSLATIONS[lang], 'lang': lang, 'last_updated': last_updated}


# ── helpers ───────────────────────────────────────────────────────────────────

def localize(item, lang):
    """Swap title/plot/genres with Hebrew versions when lang is 'he'."""
    if lang != 'he':
        return item
    item = dict(item)
    item['title'] = item.get('title_he') or item.get('title', '')
    item['plot']  = item.get('plot_he')  or item.get('plot', '')
    he_genres = item.get('genres_he', '[]')
    if he_genres and he_genres != '[]':
        item['genres'] = he_genres
    return item


def enrich(item):
    """Parse genres and streaming JSON fields into Python lists."""
    try:
        item["genres_list"] = json.loads(item["genres"])
    except Exception:
        item["genres_list"] = []
    try:
        item["streaming_list"] = json.loads(item.get("streaming") or "[]")
    except Exception:
        item["streaming_list"] = []
    return item


def current_profile():
    pid = session.get("profile_id")
    if not pid:
        return None
    row = query("SELECT id, name, avatar FROM profiles WHERE id = ?", (pid,), one=True)
    return {"id": row["id"], "name": row["name"], "avatar": row["avatar"] or '🎬'} if row else None

def require_profile():
    p = current_profile()
    if not p:
        abort(redirect(url_for("index")))
    return p


# ── routes ────────────────────────────────────────────────────────────────────

def get_all_profiles():
    rows = query("SELECT id, name, avatar FROM profiles ORDER BY id")
    return {row["id"]: {"name": row["name"], "avatar": row["avatar"] or '🎬'} for row in rows}

@app.route("/")
def index():
    profile = current_profile()
    return render_template("index.html", profiles=get_all_profiles(),
                           profile=profile, avatars=AVATARS)

@app.route("/set_lang/<lang>")
def set_lang(lang):
    if lang in ('en', 'he'):
        session['lang'] = lang
    return redirect(request.referrer or url_for('index'))


@app.route("/profile/<int:pid>/rename", methods=["POST"])
def rename_profile(pid):
    if not query("SELECT id FROM profiles WHERE id = ?", (pid,), one=True):
        abort(404)
    name = request.form.get("name", "").strip()
    if name:
        execute("UPDATE profiles SET name = ? WHERE id = ?", (name, pid))
    return redirect(url_for("index"))


@app.route("/profile/<int:pid>")
def set_profile(pid):
    if not query("SELECT id FROM profiles WHERE id = ?", (pid,), one=True):
        return redirect(url_for("index"))
    session["profile_id"] = pid
    return redirect(url_for("recommendations"))


@app.route("/profile/add", methods=["POST"])
def add_profile():
    name   = request.form.get("name", "").strip() or "New User"
    avatar = request.form.get("avatar", "🎬").strip()
    if avatar not in AVATARS:
        avatar = "🎬"
    row    = query("SELECT MAX(id) AS m FROM profiles", one=True)
    new_id = (row["m"] or 0) + 1
    execute("INSERT INTO profiles (id, name, avatar) VALUES (?, ?, ?)", (new_id, name, avatar))
    session["profile_id"] = new_id
    return redirect(url_for("recommendations"))


@app.route("/profile/<int:pid>/avatar", methods=["POST"])
def update_avatar(pid):
    if not query("SELECT id FROM profiles WHERE id = ?", (pid,), one=True):
        abort(404)
    data   = request.get_json()
    avatar = (data or {}).get("avatar", "").strip()
    if avatar not in AVATARS:
        return jsonify({"error": "invalid avatar"}), 400
    execute("UPDATE profiles SET avatar = ? WHERE id = ?", (avatar, pid))
    return jsonify({"ok": True})


@app.route("/recommendations")
def recommendations():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    genre_filter = request.args.get("genre", "").strip()

    lang = session.get('lang', 'en')
    all_recs = get_recommendations(profile["id"], limit=100)
    insights = get_genre_insights(profile["id"])

    # Localize and parse genres
    all_recs = [enrich(localize(i, lang)) for i in all_recs]

    # Genre buttons: use Hebrew genres column when in Hebrew mode
    genres_col = "genres_he" if lang == "he" else "genres"
    all_genres = sorted({
        g
        for row in query(f"SELECT {genres_col} AS genres FROM content")
        for g in json.loads(row["genres"] or "[]")
        if g
    })

    def streaming_first(items):
        return sorted(items, key=lambda x: (0 if x["streaming_list"] else 1))

    if genre_filter:
        filtered = [i for i in all_recs if genre_filter in i["genres_list"]]
        if not filtered:
            db_items = query(
                f"SELECT * FROM content WHERE {genres_col} LIKE ? ORDER BY imdb_rating DESC LIMIT 50",
                (f'%{genre_filter}%',)
            )
            filtered = [enrich(localize(dict(i), lang)) for i in db_items]
        grouped = {genre_filter: streaming_first(filtered)}
    else:
        grouped = {}
        for item in all_recs:
            primary = item["genres_list"][0] if item["genres_list"] else "Other"
            grouped.setdefault(primary, []).append(item)
        grouped = {genre: streaming_first(items) for genre, items in grouped.items()}

    return render_template("recommendations.html",
                           profile=profile, grouped=grouped,
                           all_genres=all_genres, genre_filter=genre_filter,
                           insights=insights)


@app.route("/watched")
def watched():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))
    lang = session.get('lang', 'en')
    items = get_watched(profile["id"])
    items = [enrich(localize(i, lang)) for i in items]
    return render_template("watched.html", profile=profile, items=items)


@app.route("/browse")
def browse():
    profile = current_profile()
    if not profile:
        return redirect(url_for("index"))

    genre_filter = request.args.get("genre", "")
    type_filter  = request.args.get("type", "")
    year_filter  = request.args.get("year", "")

    lang = session.get('lang', 'en')
    genres_col = "genres_he" if lang == "he" else "genres"
    sql = "SELECT * FROM content WHERE 1=1"
    params = []
    if genre_filter:
        sql += f" AND {genres_col} LIKE ?"
        params.append(f"%{genre_filter}%")
    if type_filter:
        sql += " AND content_type = ?"
        params.append(type_filter)
    if year_filter:
        sql += " AND release_year = ?"
        params.append(int(year_filter))
    sql += " ORDER BY imdb_rating DESC LIMIT 100"

    items = query(sql, params)
    items = [enrich(localize(dict(i), lang)) for i in items]

    # All unique genres for filter dropdown
    genres_col = "genres_he" if lang == "he" else "genres"
    all_genres = set()
    for row in query(f"SELECT {genres_col} AS genres FROM content"):
        try:
            for g in json.loads(row["genres"]):
                if g:
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
    lang = session.get('lang', 'en')

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

    item = enrich(localize(item, lang))

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
            results = [enrich(localize(dict(r), session.get('lang', 'en'))) for r in db_results]
            for item in results:
                item["source"] = "db"
        else:
            # Fall back to TMDB search
            tmdb_results = search_tmdb(q, language=session.get('lang', 'en'))
            for item in tmdb_results:
                item["source"] = "tmdb"
                item["id"] = None
                enrich(item)
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
    mode = request.args.get("mode", "full")

    def refresh_then_streaming():
        run_refresh()
        run_streaming_refresh()

    if mode == "streaming":
        t = threading.Thread(target=run_streaming_refresh, daemon=True)
    else:
        t = threading.Thread(target=refresh_then_streaming, daemon=True)
    t.start()
    return "Refresh started in background. Check <a href='/admin/status?secret=" + config.ADMIN_SECRET + "'>status</a> for progress."


@app.route("/admin/providers_debug")
def admin_providers_debug():
    secret = request.args.get("secret", "")
    if secret != config.ADMIN_SECRET:
        abort(403)
    from ingestion.tmdb_client import _get
    samples = query("SELECT tmdb_id, title, content_type FROM content LIMIT 20")
    results = []
    for row in samples:
        ctype = "movie" if row["content_type"] == "movie" else "tv"
        path = f"/{ctype}/{row['tmdb_id']}/watch/providers"
        try:
            data = _get(path)
            il = data.get("results", {}).get("IL", {})
            flatrate = [p["provider_name"] for p in il.get("flatrate", [])]
            results.append({"title": row["title"], "providers": flatrate})
        except Exception as e:
            results.append({"title": row["title"], "providers": [f"error: {e}"]})
    output = "<h2>Raw IL providers (first 20 titles)</h2><table border=1 cellpadding=4>"
    for r in results:
        output += f"<tr><td>{r['title']}</td><td>{', '.join(r['providers']) or '—'}</td></tr>"
    output += "</table>"
    return output


@app.route("/admin/status")
def admin_status():
    secret = request.args.get("secret", "")
    if secret != config.ADMIN_SECRET:
        abort(403)
    logs = query("SELECT * FROM refresh_log ORDER BY id DESC LIMIT 10")
    total = query("SELECT COUNT(*) as c FROM content", one=True)["c"]
    streaming_count = query(
        "SELECT COUNT(*) as c FROM content WHERE streaming IS NOT NULL AND streaming != '[]'",
        one=True
    )["c"]
    streaming_samples = query(
        "SELECT title, streaming FROM content WHERE streaming IS NOT NULL AND streaming != '[]' LIMIT 10"
    )
    # Count all unique provider names across all titles
    all_streaming_rows = query(
        "SELECT streaming FROM content WHERE streaming IS NOT NULL AND streaming != '[]'"
    )
    provider_counts = {}
    for row in all_streaming_rows:
        try:
            for p in json.loads(row["streaming"]):
                provider_counts[p] = provider_counts.get(p, 0) + 1
        except Exception:
            pass
    provider_counts = sorted(provider_counts.items(), key=lambda x: x[1], reverse=True)

    return render_template("admin_status.html", logs=logs, total=total,
                           streaming_count=streaming_count,
                           streaming_samples=streaming_samples,
                           provider_counts=provider_counts,
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
            run_streaming_refresh()
        except Exception as e:
            print(f"[refresh] Initial refresh failed: {e}")

    while True:
        time.sleep(7 * 24 * 3600)   # 7 days
        try:
            run_refresh()
            run_streaming_refresh()
        except Exception as e:
            print(f"[refresh] Scheduled refresh failed: {e}")


# ── startup ───────────────────────────────────────────────────────────────────

# Runs on every startup (python app.py AND gunicorn)
init_db()
t = threading.Thread(target=_refresh_loop, daemon=True)
t.start()

if __name__ == "__main__":
    app.run(debug=False, port=5000)

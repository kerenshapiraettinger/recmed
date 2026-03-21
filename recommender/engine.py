import json
import math
import re
from datetime import date, datetime
from db.database import query, execute, executemany

_STOPWORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
    'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'their',
    'they', 'he', 'she', 'it', 'we', 'you', 'i', 'his', 'her', 'its',
    'our', 'your', 'my', 'this', 'that', 'these', 'those', 'who', 'which',
    'what', 'when', 'where', 'how', 'why', 'not', 'no', 'if', 'into',
    'out', 'up', 'about', 'after', 'before', 'through', 'over', 'under',
    'between', 'then', 'than', 'so', 'while', 'each', 'all', 'also',
    'just', 'more', 'one', 'two', 'new', 'him', 'them', 'her', 'its',
    'find', 'life', 'only', 'when', 'after', 'story', 'must', 'make',
    'take', 'come', 'back', 'away', 'face', 'turn', 'help', 'work',
    'live', 'goes', 'sets', 'gets', 'goes', 'life', 'time', 'world',
    'show', 'want', 'left', 'soon', 'even', 'both', 'much', 'many',
}


def _keywords(text):
    """Extract meaningful words (4+ chars, non-stopword) from plot text."""
    if not text:
        return []
    return [w for w in re.findall(r'[a-z]{4,}', text.lower()) if w not in _STOPWORDS]


def rebuild_affinity(profile_id):
    rows = query(
        """SELECT r.rating, c.imdb_rating, c.genres
           FROM ratings r
           JOIN content c ON c.id = r.content_id
           WHERE r.profile_id = ?""",
        (profile_id,)
    )
    if not rows:
        return

    genre_deltas = {}
    for row in rows:
        user_rating = row["rating"]
        imdb_rating = row["imdb_rating"] or 7.0
        delta = user_rating - imdb_rating
        try:
            genres = json.loads(row["genres"])
        except (ValueError, TypeError):
            genres = []
        for genre in genres:
            genre_deltas.setdefault(genre, []).append(delta)

    now = datetime.utcnow().isoformat()
    affinity_rows = []
    for genre, deltas in genre_deltas.items():
        mean_delta = sum(deltas) / len(deltas)
        score = mean_delta * math.log1p(len(deltas))
        affinity_rows.append((profile_id, genre, score, len(deltas), now))

    execute("DELETE FROM genre_affinity WHERE profile_id = ?", (profile_id,))
    if affinity_rows:
        executemany(
            """INSERT INTO genre_affinity (profile_id, genre, score, sample_size, updated_at)
               VALUES (?,?,?,?,?)""",
            affinity_rows
        )


def get_recommendations(profile_id, limit=20):
    """
    Returns {'imdb': [...], 'seret': [...]} — two ranked lists.
    IMDb list: all unrated titles scored using imdb_rating.
    Seret list: only titles with seret_rating >= 7 from last 5 years,
                scored using seret_rating instead of imdb_rating.
    """
    rating_rows = query(
        """SELECT r.rating, r.content_id, c.imdb_rating, c.genres, c.plot, c.director
           FROM ratings r
           JOIN content c ON c.id = r.content_id
           WHERE r.profile_id = ?""",
        (profile_id,)
    )
    n_ratings = len(rating_rows)

    # Phase-based weights
    if n_ratings < 10:
        w_genre, w_kw, w_dir, w_imdb = 0.70, 0.00, 0.00, 0.30
    elif n_ratings < 30:
        w_genre, w_kw, w_dir, w_imdb = 0.55, 0.25, 0.00, 0.20
    else:
        w_genre, w_kw, w_dir, w_imdb = 0.45, 0.30, 0.10, 0.15

    affinity_rows = query(
        "SELECT genre, score FROM genre_affinity WHERE profile_id = ?", (profile_id,)
    )
    affinity = {row["genre"]: row["score"] for row in affinity_rows}

    rated_ids = {row["content_id"] for row in rating_rows}

    any_rated_ids = {row["content_id"] for row in query(
        "SELECT DISTINCT content_id FROM ratings"
    )}

    # Keyword affinity from rated plots (threshold: 3+ rated titles)
    kw_deltas = {}
    for row in rating_rows:
        delta = row["rating"] - (row["imdb_rating"] or 7.0)
        for kw in set(_keywords(row["plot"] or "")):
            kw_deltas.setdefault(kw, []).append(delta)
    kw_affinity = {
        kw: (sum(d) / len(d)) * math.log1p(len(d))
        for kw, d in kw_deltas.items()
        if len(d) >= 3
    }

    # Director affinity (threshold: 2+ rated titles)
    dir_deltas = {}
    for row in rating_rows:
        director = (row.get("director") or "").strip()
        if director:
            delta = row["rating"] - (row["imdb_rating"] or 7.0)
            dir_deltas.setdefault(director, []).append(delta)
    dir_affinity = {
        d: (sum(v) / len(v)) * math.log1p(len(v))
        for d, v in dir_deltas.items()
        if len(v) >= 2
    }

    all_content = query("SELECT * FROM content ORDER BY imdb_rating DESC")

    current_year = date.today().year
    seret_min_year = current_year - 5

    imdb_scored = []
    seret_scored = []

    for item in all_content:
        if item["id"] in rated_ids:
            continue

        try:
            genres = json.loads(item["genres"])
        except (ValueError, TypeError):
            genres = []

        genre_score = sum(affinity.get(g, 0) for g in genres)

        kw_score = 0.0
        if w_kw > 0 and kw_affinity:
            for kw in set(_keywords(item["plot"] or "")):
                kw_score += kw_affinity.get(kw, 0)

        dir_score = 0.0
        if w_dir > 0 and dir_affinity:
            director = (item.get("director") or "").strip()
            if director:
                dir_score = dir_affinity.get(director, 0)

        age_penalty = 0.1 * max(0, current_year - item["release_year"])

        # IMDb-based score
        imdb_bonus = (item["imdb_rating"] or 0) if item["id"] in any_rated_ids else 0
        imdb_score = (
            w_genre * genre_score
            + w_kw * kw_score
            + w_dir * dir_score
            + w_imdb * imdb_bonus
            - age_penalty
        )
        imdb_scored.append((imdb_score, item))

        # Seret-based score: only titles with seret_rating >= 7 from last 5 years
        seret_rating = item.get("seret_rating")
        if seret_rating and seret_rating >= 7.0 and (item.get("release_year") or 0) >= seret_min_year:
            seret_bonus = seret_rating if item["id"] in any_rated_ids else 0
            seret_score = (
                w_genre * genre_score
                + w_kw * kw_score
                + w_dir * dir_score
                + w_imdb * seret_bonus
                - age_penalty
            )
            seret_scored.append((seret_score, item))

    imdb_scored.sort(key=lambda x: x[0], reverse=True)
    seret_scored.sort(key=lambda x: x[0], reverse=True)

    return {
        "imdb": [item for _, item in imdb_scored[:limit]],
        "seret": [item for _, item in seret_scored[:limit]],
    }


def get_genre_insights(profile_id):
    rows = query(
        """SELECT genre, score, sample_size
           FROM genre_affinity
           WHERE profile_id = ?
           ORDER BY score DESC""",
        (profile_id,)
    )
    return rows


def get_watched(profile_id):
    rows = query(
        """SELECT c.*, r.rating, r.rated_at
           FROM ratings r
           JOIN content c ON c.id = r.content_id
           WHERE r.profile_id = ?
           ORDER BY r.rated_at DESC""",
        (profile_id,)
    )
    return rows

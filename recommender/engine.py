import json
import math
from datetime import date, datetime
from db.database import query, execute, executemany


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
    affinity_rows = query(
        "SELECT genre, score FROM genre_affinity WHERE profile_id = ?", (profile_id,)
    )
    affinity = {row["genre"]: row["score"] for row in affinity_rows}

    rated_ids = {row["content_id"] for row in query(
        "SELECT content_id FROM ratings WHERE profile_id = ?", (profile_id,)
    )}

    any_rated_ids = {row["content_id"] for row in query(
        "SELECT DISTINCT content_id FROM ratings"
    )}

    all_content = query("SELECT * FROM content ORDER BY imdb_rating DESC")

    current_year = date.today().year
    scored = []
    for item in all_content:
        if item["id"] in rated_ids:
            continue
        try:
            genres = json.loads(item["genres"])
        except (ValueError, TypeError):
            genres = []
        genre_score = sum(affinity.get(g, 0) for g in genres)
        imdb_bonus = 0.3 * (item["imdb_rating"] or 0) if item["id"] in any_rated_ids else 0
        age_penalty = 0.1 * max(0, current_year - item["release_year"])
        score = genre_score + imdb_bonus - age_penalty
        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:limit]]


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

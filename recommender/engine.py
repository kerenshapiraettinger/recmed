import json
import math
from datetime import date, datetime
from db.database import get_connection, executemany, execute

def rebuild_affinity(profile_id):
    """
    Recompute genre_affinity for a profile from scratch.
    Called after every new rating submission.

    affinity(genre) = mean(user_rating - imdb_rating for all rated titles in genre)
                    * log1p(count)
    """
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT r.rating, c.imdb_rating, c.genres
               FROM ratings r
               JOIN content c ON c.id = r.content_id
               WHERE r.profile_id = ?""",
            (profile_id,)
        ).fetchall()

    if not rows:
        return

    genre_deltas = {}  # genre -> list of deltas
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
        confidence = math.log1p(len(deltas))
        score = mean_delta * confidence
        affinity_rows.append((profile_id, genre, score, len(deltas), now))

    with get_connection() as conn:
        conn.execute("DELETE FROM genre_affinity WHERE profile_id = ?", (profile_id,))
        conn.executemany(
            """INSERT INTO genre_affinity (profile_id, genre, score, sample_size, updated_at)
               VALUES (?,?,?,?,?)""",
            affinity_rows
        )
        conn.commit()


def get_recommendations(profile_id, limit=20):
    """
    Return ranked list of content dicts not yet rated by this profile.

    score = sum(affinity[g] for g in title.genres)
          + 0.3 * imdb_rating
          - 0.1 * age_in_years
    """
    with get_connection() as conn:
        affinity_rows = conn.execute(
            "SELECT genre, score FROM genre_affinity WHERE profile_id = ?",
            (profile_id,)
        ).fetchall()
        affinity = {row["genre"]: row["score"] for row in affinity_rows}

        rated_ids = {
            row[0] for row in conn.execute(
                "SELECT content_id FROM ratings WHERE profile_id = ?", (profile_id,)
            ).fetchall()
        }

        all_content = conn.execute(
            "SELECT * FROM content ORDER BY imdb_rating DESC"
        ).fetchall()

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
        imdb_bonus = 0.3 * (item["imdb_rating"] or 0)
        age_penalty = 0.1 * max(0, current_year - item["release_year"])
        score = genre_score + imdb_bonus - age_penalty

        scored.append((score, dict(item)))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:limit]]


def get_genre_insights(profile_id):
    """
    Return genre affinity data for display in the profile page.
    Returns list of (genre, score, sample_size) sorted by score desc.
    """
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT genre, score, sample_size
               FROM genre_affinity
               WHERE profile_id = ?
               ORDER BY score DESC""",
            (profile_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_watched(profile_id):
    """Return all titles rated by a profile, newest first."""
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT c.*, r.rating, r.rated_at
               FROM ratings r
               JOIN content c ON c.id = r.content_id
               WHERE r.profile_id = ?
               ORDER BY r.rated_at DESC""",
            (profile_id,)
        ).fetchall()
    return [dict(r) for r in rows]

import requests
import json
from datetime import datetime, date
from config import TMDB_API_KEY, MIN_IMDB_RATING, MIN_VOTE_COUNT, CONTENT_YEARS

BASE_URL = "https://api.themoviedb.org/3"
POSTER_BASE = "https://image.tmdb.org/t/p/w342"

def _get(path, params=None):
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    r = requests.get(BASE_URL + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_genre_map(content_type="movie"):
    """Returns {genre_id: genre_name}"""
    endpoint = "/genre/movie/list" if content_type == "movie" else "/genre/tv/list"
    data = _get(endpoint)
    return {g["id"]: g["name"] for g in data["genres"]}

def discover(content_type="movie", genre_map=None):
    """
    Yields title dicts for all pages matching our criteria.
    content_type: 'movie' or 'tv'
    """
    endpoint = "/discover/movie" if content_type == "movie" else "/discover/tv"
    today = date.today()
    cutoff_year = today.year - CONTENT_YEARS

    if content_type == "movie":
        date_field_gte = "primary_release_date.gte"
        date_field_lte = "primary_release_date.lte"
        date_gte = f"{cutoff_year}-01-01"
        date_lte = today.isoformat()
        year_field = "release_date"
    else:
        date_field_gte = "first_air_date.gte"
        date_field_lte = "first_air_date.lte"
        date_gte = f"{cutoff_year}-01-01"
        date_lte = today.isoformat()
        year_field = "first_air_date"

    params = {
        date_field_gte: date_gte,
        date_field_lte: date_lte,
        "vote_average.gte": MIN_IMDB_RATING,
        "vote_count.gte": MIN_VOTE_COUNT,
        "sort_by": "vote_average.desc",
        "page": 1,
    }

    while True:
        data = _get(endpoint, params)
        results = data.get("results", [])
        if not results:
            break

        for item in results:
            raw_year = item.get(year_field, "")
            try:
                year = int(raw_year[:4])
            except (ValueError, TypeError):
                year = today.year

            genres = []
            if genre_map:
                genres = [genre_map[gid] for gid in item.get("genre_ids", []) if gid in genre_map]

            poster_path = item.get("poster_path")
            title = item.get("title") or item.get("name", "")

            yield {
                "tmdb_id": item["id"],
                "title": title,
                "content_type": "movie" if content_type == "movie" else "series",
                "release_year": year,
                "imdb_rating": round(item.get("vote_average", 0), 1),
                "genres": json.dumps(genres),
                "poster_url": POSTER_BASE + poster_path if poster_path else None,
                "last_refreshed": today.isoformat(),
            }

        total_pages = data.get("total_pages", 1)
        if params["page"] >= total_pages or params["page"] >= 20:  # cap at 20 pages
            break
        params["page"] += 1

def fetch_imdb_id(tmdb_id, content_type="movie"):
    """Fetch IMDb ID for a TMDB title (used for OMDb enrichment)."""
    if content_type == "movie":
        path = f"/movie/{tmdb_id}/external_ids"
    else:
        path = f"/tv/{tmdb_id}/external_ids"
    try:
        data = _get(path)
        return data.get("imdb_id")
    except Exception:
        return None

def search_tmdb(query, content_type=None):
    """Search TMDB for movies and/or series by title."""
    results = []
    types = ["movie", "tv"] if content_type is None else [content_type]
    for ctype in types:
        endpoint = "/search/movie" if ctype == "movie" else "/search/tv"
        data = _get(endpoint, {"query": query})
        for item in data.get("results", [])[:5]:
            poster_path = item.get("poster_path")
            raw_year = item.get("release_date") or item.get("first_air_date", "")
            try:
                year = int(raw_year[:4])
            except (ValueError, TypeError):
                year = 0
            results.append({
                "tmdb_id": item["id"],
                "title": item.get("title") or item.get("name", ""),
                "content_type": "movie" if ctype == "movie" else "series",
                "release_year": year,
                "imdb_rating": round(item.get("vote_average", 0), 1),
                "poster_url": POSTER_BASE + poster_path if poster_path else None,
                "genres": json.dumps([]),
            })
    return results

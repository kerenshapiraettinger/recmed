from ingestion.tmdb_client import _get

KAN11_NETWORK_ID = 2970


def get_kan11_tmdb_ids():
    """
    Fetch all TMDB IDs for TV shows on the Kan 11 network (network ID 2970).
    Returns a set of TMDB IDs.
    """
    tmdb_ids = set()
    page = 1
    while True:
        try:
            data = _get("/discover/tv", {
                "with_networks": KAN11_NETWORK_ID,
                "page": page,
            })
        except Exception as e:
            print(f"[kan11] TMDB network fetch error (page {page}): {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            tmdb_ids.add(item["id"])

        total_pages = data.get("total_pages", 1)
        if page >= total_pages or page >= 20:
            break
        page += 1

    print(f"[kan11] Found {len(tmdb_ids)} Kan 11 titles on TMDB")
    return tmdb_ids


def match_kan11(db_titles_he):
    """
    Given a list of (id, title_he) tuples from the DB (unused, kept for
    interface compatibility), return the set of content IDs that are on
    Kan 11 according to TMDB network data.

    Matches by tmdb_id against the DB.
    """
    from db.database import query

    kan11_tmdb_ids = get_kan11_tmdb_ids()
    if not kan11_tmdb_ids:
        return set()

    # Find which DB content rows have a tmdb_id in the Kan 11 set
    rows = query("SELECT id, tmdb_id FROM content")
    matched_ids = {row["id"] for row in rows if row["tmdb_id"] in kan11_tmdb_ids}

    print(f"[kan11] Matched {len(matched_ids)} titles in DB")
    return matched_ids

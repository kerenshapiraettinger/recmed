import requests
from config import OMDB_API_KEY

BASE_URL = "http://www.omdbapi.com/"

def fetch_plot(imdb_id):
    """Fetch plot text from OMDb by IMDb ID. Returns None if unavailable."""
    if not OMDB_API_KEY or not imdb_id:
        return None
    try:
        r = requests.get(BASE_URL, params={"i": imdb_id, "apikey": OMDB_API_KEY, "plot": "short"}, timeout=8)
        data = r.json()
        if data.get("Response") == "True":
            return data.get("Plot")
    except Exception:
        pass
    return None

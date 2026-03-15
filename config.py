import os
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY    = os.getenv("TMDB_API_KEY", "")
OMDB_API_KEY    = os.getenv("OMDB_API_KEY", "")
SECRET_KEY      = os.getenv("SECRET_KEY", "change-me-in-production")
ADMIN_SECRET    = os.getenv("ADMIN_SECRET", "admin")

DB_PATH         = os.path.join(os.path.dirname(__file__), "data", "recmed.db")

MIN_IMDB_RATING = 7.0
MIN_VOTE_COUNT  = 150   # filter low-sample noise on TMDB
CONTENT_YEARS   = 5     # rolling window in years

PROFILES = {
    1: "User 1",
    2: "User 2",
    3: "User 3",
    4: "User 4",
    5: "User 5",
    6: "User 6",
}

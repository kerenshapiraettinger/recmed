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
    1: os.getenv("PROFILE_1_NAME", "User 1"),
    2: os.getenv("PROFILE_2_NAME", "User 2"),
    3: os.getenv("PROFILE_3_NAME", "User 3"),
    4: os.getenv("PROFILE_4_NAME", "User 4"),
    5: os.getenv("PROFILE_5_NAME", "User 5"),
    6: os.getenv("PROFILE_6_NAME", "User 6"),
}

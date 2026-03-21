import re
import time
import unicodedata

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.seret.co.il/",
}
MIN_VOTES = 10


def _fetch(url, params=None, data=None):
    if data:
        r = requests.post(url, headers=HEADERS, data=data, timeout=15)
    else:
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    r.encoding = "windows-1255"
    return BeautifulSoup(r.text, "html.parser")


def _normalize(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'[\s\-\u2013\u2014\u05f4\u05f3"\'.,;:!?()\[\]]+', "", text)
    return text.strip()


def _search(title_he):
    """Search seret.co.il and return list of (mid, title) tuples."""
    try:
        encoded = title_he.encode("windows-1255", errors="replace")
        soup = _fetch(
            "https://www.seret.co.il/movies/l_movies.asp",
            data={"searchbar": encoded, "f_sType": "movie"},
        )
        seen = set()
        results = []
        for a in soup.find_all("a", href=re.compile(r"s_movies\.asp\?MID=\d+", re.I)):
            m = re.search(r"MID=(\d+)", a["href"], re.I)
            if m:
                mid = int(m.group(1))
                if mid not in seen:
                    seen.add(mid)
                    results.append((mid, a.get_text(strip=True)))
        return results
    except Exception as e:
        print(f"[seret] Search error for '{title_he}': {e}")
        return []


def _fetch_page(mid):
    """Fetch a seret title page. Returns (rating, votes, year, title_he)."""
    try:
        soup = _fetch(
            "https://www.seret.co.il/movies/s_movies.asp",
            params={"MID": mid},
        )
        og = soup.find("meta", property="og:title")
        title_he = og["content"].strip() if og else ""

        agg = soup.find(attrs={"itemprop": "aggregateRating"})
        if not agg:
            return None, 0, None, title_he

        rv = agg.find(attrs={"itemprop": "ratingValue"})
        if not rv:
            return None, 0, None, title_he
        try:
            rating = float(rv.get_text(strip=True))
        except (ValueError, TypeError):
            return None, 0, None, title_he

        votes = 0
        rc = agg.find("meta", attrs={"itemprop": "reviewCount"})
        if rc:
            try:
                votes = int(rc.get("content", 0))
            except (ValueError, TypeError):
                pass

        year = None
        dp = soup.find(attrs={"itemprop": "datePublished"})
        if dp:
            m = re.search(r"(\d{4})", dp.get_text(strip=True))
            if m:
                try:
                    year = int(m.group(1))
                except ValueError:
                    pass

        return rating, votes, year, title_he
    except Exception as e:
        print(f"[seret] Fetch error for MID {mid}: {e}")
        return None, 0, None, None


def find_seret_rating(title_he, release_year=None):
    """
    Search seret.co.il for a Hebrew title and return the best match.
    Returns (seret_id, seret_rating, seret_votes) or (None, None, None).
    Requires at least MIN_VOTES votes to trust the rating.
    """
    candidates = _search(title_he)
    if not candidates:
        return None, None, None

    norm_query = _normalize(title_he)

    for mid, _ in candidates[:5]:
        rating, votes, year, page_title = _fetch_page(mid)
        time.sleep(0.3)

        if rating is None or votes < MIN_VOTES:
            continue

        # Title similarity check
        if page_title:
            norm_page = _normalize(page_title)
            if norm_query and norm_page:
                if norm_query not in norm_page and norm_page not in norm_query:
                    if len(norm_query) >= 4 and norm_query[:4] not in norm_page:
                        continue

        # Year check (±1 year tolerance)
        if release_year and year and abs(release_year - year) > 1:
            continue

        return mid, rating, votes

    return None, None, None

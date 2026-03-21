import requests
from bs4 import BeautifulSoup
import unicodedata
import re

KAN_URL = "https://www.kan.org.il/lobby/kan-box/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _normalize(text):
    """Strip punctuation/whitespace for loose matching."""
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"[\s\-–—״׳\"'.,:;!?()]", "", text)
    return text.strip()


def fetch_kan11_titles():
    """Scrape Kan 11 Box page and return a set of normalized Hebrew titles."""
    try:
        r = requests.get(KAN_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[kan11] Failed to fetch page: {e}")
        return set(), set()

    soup = BeautifulSoup(r.text, "html.parser")
    titles = set()

    # img alt attributes (most reliable — poster images carry the show name)
    for img in soup.find_all("img", alt=True):
        alt = img["alt"].strip()
        if alt and any("\u0590" <= c <= "\u05FF" for c in alt):  # contains Hebrew
            titles.add(alt)

    # h2 / h3 headings inside cards
    for tag in soup.find_all(["h2", "h3"]):
        text = tag.get_text(strip=True)
        if text and any("\u0590" <= c <= "\u05FF" for c in text):
            titles.add(text)

    normalized = {_normalize(t): t for t in titles}
    print(f"[kan11] Found {len(titles)} Hebrew titles on Kan Box page")
    return titles, normalized


def match_kan11(db_titles_he):
    """
    Given a list of (id, title_he) tuples from the DB,
    return the set of content IDs that match a Kan 11 title.
    """
    _, normalized_kan = fetch_kan11_titles()
    if not normalized_kan:
        return set()

    matched_ids = set()
    for content_id, title_he in db_titles_he:
        if not title_he:
            continue
        norm = _normalize(title_he)
        if norm in normalized_kan:
            matched_ids.add(content_id)

    print(f"[kan11] Matched {len(matched_ids)} titles in DB")
    return matched_ids

import re
import time
import random
import threading
import requests
from bs4 import BeautifulSoup
from sqlite_helper import SQLiteHelper
from metrics_collector import MetricsCollector

# ─────────────────────────────────────────────────────────────────────────────
# AsuraComicHelper
#
# Scrapes https://asuracomic.net/ with plain requests + BeautifulSoup.
# No Cloudflare protection is present, so no browser driver is required.
#
# Public interface (mirrors MangaDexHelper exactly):
#   get_recent_manga(offset)      -> list[dict]
#   get_requested_manga(manga_id) -> dict
#   set_latest_chapters(manga)    -> bool
#   download_chapters(manga)      -> None
#
# ID / Hash strategy
# ──────────────────
# Each manga is assigned a deterministic ID in the format:
#   as-XXXXXXXX-XXXXXXXX
# where the two 8-character hex groups are built from the ASCII values of the
# manga title's characters (each char → 2-digit lowercase hex, concatenated,
# then split into the first 8 and next 8 hex digits, zero-padded if needed).
#
# Example: "Nano Machine"
#   N=4e  a=61  n=6e  o=6f  (space)=20  M=4d  a=61  c=63  h=68  i=69  n=6e  e=65
#   hex string → "4e616e6f204d6163..."
#   ID         → "as-4e616e6f-204d6163"
#
# Because the ID no longer embeds the slug, AsuraComicHelper keeps an internal
# {hash → slug} map (self._slug_map) populated during get_recent_manga() so
# that set_latest_chapters() and download_chapters() can still build URLs.
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL       = "https://asuracomic.net"
ITEMS_PER_PAGE = 20   # series cards per listing page

# Shared session with browser-like headers.
# Do NOT set Accept-Encoding manually — requests handles gzip/deflate
# decompression automatically. Advertising brotli (br) without the brotli
# package installed causes the server to send content requests cannot decode.
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
})


# ── Module-level helpers ──────────────────────────────────────────────────────

def _make_hash(title: str) -> str:
    """
    Build a deterministic ID from the manga title's ASCII values.

    Each character is converted to its 2-digit lowercase hex ASCII value.
    The resulting string is split into two 8-character groups (zero-padded
    to 16 chars if the title is very short), yielding:
        as-XXXXXXXX-XXXXXXXX
    """
    hex_str = "".join(f"{ord(c):02x}" for c in title).ljust(16, "0")
    return f"as-{hex_str[:8]}-{hex_str[8:16]}"


def _normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().replace("\x00", "")


def _slug_from_url(href: str) -> str:
    """Extract the series slug from any AsuraComic series URL."""
    m = re.search(r"/series/([^/?#]+)", href)
    return m.group(1) if m else None


def warmup_session() -> bool:
    """
    Call this ONCE from main.py before starting any worker threads.
    Hits the homepage to acquire session cookies and verify connectivity.
    Returns True if the site is reachable, False otherwise.
    """
    try:
        resp = _SESSION.get(BASE_URL, timeout=10)
        print(f"[AsuraComic] Warmup -> {resp.status_code}  (cookies: {dict(resp.cookies)})")
        if resp.status_code == 200:
            return True
        print(f"[AsuraComic] WARNING: Warmup returned {resp.status_code}. "
              f"The site may be blocking server/datacenter IPs.")
        return False
    except requests.Timeout:
        print("[AsuraComic] Warmup TIMED OUT – the site may be blocking this IP. "
              "Consider using a residential proxy.")
        return False
    except Exception as exc:
        print(f"[AsuraComic] Warmup failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────

class AsuraComicHelper:

    def __init__(self):
        self.db        = SQLiteHelper()
        self.metrics   = MetricsCollector()
        # Maps  hash → slug  so we can reconstruct URLs after the ID loses the slug
        self._slug_map: dict[str, str] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP fetch
    # ─────────────────────────────────────────────────────────────────────────

    def _get_html(self, url: str, retries: int = 3) -> str:
        for attempt in range(retries):
            try:
                resp = _SESSION.get(url, timeout=10)
                resp.raise_for_status()
                print(f"[AsuraComic] GET {url} -> {resp.status_code} ({len(resp.content)} bytes)")
                return resp.text
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response else "unknown"
                if status == 429:
                    wait = 60
                    print(f"[AsuraComic] Rate-limited (429) – sleeping {wait}s")
                    self.metrics.record_error("rate_limits")
                else:
                    wait = 3 * (attempt + 1) + random.uniform(0, 2)
                    print(f"[AsuraComic] HTTP {status} on {url} (attempt {attempt + 1}) – retrying in {wait:.1f}s")
                    self.metrics.record_error("api_errors")
                time.sleep(wait)
            except requests.Timeout:
                wait = 3 * (attempt + 1) + random.uniform(0, 2)
                print(f"[AsuraComic] Timeout on {url} (attempt {attempt + 1}) – retrying in {wait:.1f}s")
                print(f"[AsuraComic] NOTE: Repeated timeouts may indicate the server is blocking this IP.")
                self.metrics.record_error("api_errors")
                time.sleep(wait)
            except requests.ConnectionError as exc:
                wait = 3 * (attempt + 1) + random.uniform(0, 2)
                print(f"[AsuraComic] Connection error on {url} (attempt {attempt + 1}): {exc} – retrying in {wait:.1f}s")
                self.metrics.record_error("api_errors")
                time.sleep(wait)
            except Exception as exc:
                wait = 3 * (attempt + 1) + random.uniform(0, 2)
                print(f"[AsuraComic] Unexpected {type(exc).__name__} on {url} (attempt {attempt + 1}): {exc} – retrying in {wait:.1f}s")
                self.metrics.record_error("api_errors")
                time.sleep(wait)

        print(f"[AsuraComic] Gave up after {retries} attempts: {url}")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Slug lookup (needed because the hash no longer embeds the slug)
    # ─────────────────────────────────────────────────────────────────────────

    def _slug_for(self, manga_id: str) -> str:
        slug = self._slug_map.get(manga_id)
        if not slug:
            print(f"[AsuraComic] WARNING: no slug cached for {manga_id}")
        return slug

    # ─────────────────────────────────────────────────────────────────────────
    # Listing page  –  /series?page=N
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_list_page(self, soup: BeautifulSoup) -> list[dict]:
        manga_list: list[dict] = []
        seen_slugs: set        = set()

        for anchor in soup.select("a[href*='/series/']"):
            try:
                href = anchor.get("href", "")
                # Skip chapter-level links  (/series/<slug>/chapter/N)
                if "/chapter/" in href:
                    continue

                slug = _slug_from_url(href)
                if not slug or slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title_tag = (
                    anchor.select_one("span.font-bold")
                    or anchor.select_one("div.font-bold")
                    or anchor.select_one("h3")
                    or anchor.select_one("h2")
                )
                title = _normalize(
                    title_tag.get_text() if title_tag
                    else anchor.get("aria-label", slug.replace("-", " ").title())
                )

                manga_id = _make_hash(title)
                # Cache slug so set_latest_chapters / download_chapters can build URLs
                self._slug_map[manga_id] = slug

                img   = anchor.select_one("img")
                cover = ""
                if img:
                    cover = (
                        img.get("src")
                        or img.get("data-src")
                        or img.get("data-lazy-src")
                        or ""
                    )

                manga_list.append({
                    "id":          manga_id,
                    "title":       title,
                    "description": "",   # populated on detail fetch
                    "cover_img":   cover,
                    "tags":        [],   # populated on detail fetch
                })
            except Exception as exc:
                print(f"[AsuraComic] Card parse error: {exc}")

        return manga_list

    # ─────────────────────────────────────────────────────────────────────────
    # Detail page  –  /series/<slug>
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_detail_page(self, soup: BeautifulSoup, manga_id: str) -> dict:
        try:
            # ── Title ─────────────────────────────────────────────────────────
            # <span class="text-xl font-bold">...</span>
            title_tag = soup.find(
                "span",
                class_=lambda c: c and "text-xl" in c.split() and "font-bold" in c.split()
            ) or soup.find("h1")
            title = _normalize(title_tag.get_text()) if title_tag else "Unknown Title"

            # ── Cover ─────────────────────────────────────────────────────────
            cover_tag = soup.find("img", alt="poster") or soup.select_one("div.relative img")
            cover = ""
            if cover_tag:
                cover = cover_tag.get("src") or cover_tag.get("data-src") or ""

            # ── Description ───────────────────────────────────────────────────
            # <span class="font-medium text-sm text-[#A2A2A2]">...</span>
            # The class text-[#A2A2A2] contains brackets that break CSS selectors,
            # so we use find() with a lambda to match on partial class membership.
            desc_tag = soup.find(
                "span",
                class_=lambda c: c and "font-medium" in c.split()
                                     and "text-sm" in c.split()
                                     and any("A2A2A2" in cls for cls in c.split())
            )
            description = _normalize(desc_tag.get_text()) if desc_tag else ""

            # ── Genres ────────────────────────────────────────────────────────
            # Genres are rendered as <button> elements inside a flex-wrap div.
            # Example: <div class="flex flex-row flex-wrap gap-3"><button ...>Action</button>
            genres_div = soup.find(
                "div",
                class_=lambda c: c and "flex-wrap" in c.split() and "gap-3" in c.split()
            )
            seen_tags: set  = set()
            tags: list[str] = []
            if genres_div:
                for btn in genres_div.find_all("button"):
                    t = _normalize(btn.get_text())
                    if t and t not in seen_tags:
                        seen_tags.add(t)
                        tags.append(t)

            return {
                "id":          manga_id,
                "title":       title,
                "description": description,
                "cover_img":   cover,
                "tags":        tags,
            }
        except Exception as exc:
            print(f"[AsuraComic] Detail parse error: {exc}")
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Chapter list  –  embedded in the detail page
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_chapter_list(self, soup: BeautifulSoup, slug: str) -> list[dict]:
        """
        Returns chapters shaped identically to MangaDex feed items:
            { "id": <full_chapter_url>, "attributes": {"chapter": "<num>"} }
        """
        chapter_anchors = (
            soup.select(f"a[href*='/series/{slug}/chapter/']")
            or soup.select("a[href*='/chapter/']")
        )

        chapters: list[dict] = []
        seen_nums: set        = set()

        for a in chapter_anchors:
            href = a.get("href", "")
            m    = re.search(r"/chapter/([\d]+(?:[._-][\d]+)?)", href)
            if not m:
                continue
            # Normalise separators: "1-5" / "1_5"  →  "1.5"
            ch_num = m.group(1).replace("_", ".").replace("-", ".")
            if ch_num in seen_nums:
                continue
            seen_nums.add(ch_num)

            # Always reconstruct from canonical parts — never trust the raw href path.
            full_url = f"{BASE_URL}/series/{slug}/chapter/{m.group(1)}"
            chapters.append({
                "id":         full_url,
                "attributes": {"chapter": ch_num},
            })

        chapters.sort(key=lambda c: float(c["attributes"]["chapter"]))
        return chapters

    # ─────────────────────────────────────────────────────────────────────────
    # Chapter image URLs
    # ─────────────────────────────────────────────────────────────────────────

    def _get_chapter_page_urls(self, chapter_url: str) -> list[str]:
        html = self._get_html(chapter_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")

        # Primary: pages are <img alt="chapter page N"> inside <div class="w-full mx-auto center">
        images = (
            soup.select("img[alt^='chapter page']")
            or soup.select("div.center img")
            or soup.select("div#chapter-container img")
            or soup.select("div.chapter-content img")
            or soup.select("div.reading-content img")
        )

        seen: set       = set()
        urls: list[str] = []
        for img in images:
            src = (
                img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            ).strip()
            if src.startswith("http") and src not in seen:
                seen.add(src)
                urls.append(src)

        # Fallback – any image served from the Asura CDN (gg.asuracomic.net)
        if not urls:
            for img in soup.select("img[src*='gg.asuracomic.net']"):
                src = (img.get("src") or "").strip()
                if src.startswith("http") and src not in seen:
                    seen.add(src)
                    urls.append(src)

        print(f"[AsuraComic] Found {len(urls)} page URLs in {chapter_url}")
        return urls

    # ─────────────────────────────────────────────────────────────────────────
    # Public API  (mirrors MangaDexHelper)
    # ─────────────────────────────────────────────────────────────────────────

    def get_recent_manga(self, offset: int) -> list[dict]:
        page = (offset // ITEMS_PER_PAGE) + 1
        url  = f"{BASE_URL}/series?page={page}"

        print(f"[AsuraComic] Fetching listing page {page} (offset={offset})")
        html = self._get_html(url)
        if not html:
            return []

        self.metrics.record_api_call("manga_list")
        soup = BeautifulSoup(html, "html.parser")
        return self._parse_list_page(soup)

    def get_requested_manga(self, manga_id: str) -> dict:
        slug = self._slug_for(manga_id)
        if not slug:
            return None
        url = f"{BASE_URL}/series/{slug}"

        print(f"[AsuraComic] Fetching detail: {url}")
        html = self._get_html(url)
        if not html:
            self.metrics.record_error("api_errors")
            return None

        self.metrics.record_api_call("manga_list")
        soup = BeautifulSoup(html, "html.parser")
        return self._parse_detail_page(soup, manga_id)

    def set_latest_chapters(self, manga) -> bool:
        """
        Fetch the chapter list for *manga*, attach it to the manga object,
        and return True if there are new chapters to download.
        Matches MangaDexHelper.set_latest_chapters() contract exactly.
        """
        slug = self._slug_for(manga.get_id())
        if not slug:
            return False
        url = f"{BASE_URL}/series/{slug}"

        # Use the hash directly as the table name (hyphens → underscores)
        table_name      = manga.get_id().replace("-", "_")
        existing_latest = self.db.get_manga_latest_chapter(table_name, manga.get_id())

        time.sleep(random.uniform(1, 4))

        print(f"[AsuraComic] Fetching chapters for {manga.get_id()} ({slug})")
        html = self._get_html(url)
        if not html:
            return False

        self.metrics.record_api_call("chapter_feed")
        soup     = BeautifulSoup(html, "html.parser")

        # Also grab description + tags here since we have the detail page loaded
        detail = self._parse_detail_page(soup, manga.get_id())
        if detail:
            if not manga.get_description() and detail.get("description"):
                manga.set_description(detail["description"])
            if not manga.get_tags() and detail.get("tags"):
                manga.set_tags(detail["tags"])

        chapters = self._parse_chapter_list(soup, slug)

        if not chapters:
            print(f"[AsuraComic] No chapters found for {manga.get_id()}")
            return False

        class _MockResponse:
            def __init__(self, data):
                self._data = data
            def json(self):
                return {"data": self._data}

        manga.set_chapters(_MockResponse(chapters))
        should_download = manga.set_latest_chapter()

        if existing_latest is not None:
            if manga.get_latest_chapter() <= existing_latest:
                print(f"[AsuraComic] No new chapters for {manga.get_id()} "
                      f"(latest={manga.get_latest_chapter()}, db={existing_latest})")
                return False
            print(f"[AsuraComic] New chapters for {manga.get_id()} "
                  f"(latest={manga.get_latest_chapter()}, db={existing_latest})")

        return should_download

    def download_chapters(self, manga):
        """
        Iterate chapters and store only missing pages to the DB.
        The page-URL table is named after the manga hash (manga.get_id()).
        Matches MangaDexHelper.download_chapters() contract exactly.
        """
        existing_chapters_status = self.db.get_chapters_with_status(manga.get_id())
        print(f"[AsuraComic] {len(existing_chapters_status)} existing chapters in DB "
              f"for {manga.get_id()}")

        worker_id = (
            threading.current_thread().name.split("-")[0]
            if "-" in threading.current_thread().name else 0
        )

        for chapter in manga.get_chapters():
            chapter_num = chapter["attributes"]["chapter"].replace(".", "_")
            chapter_url = chapter["id"]

            existing_pages: set = set()
            is_new_chapter = chapter_num not in existing_chapters_status
            if chapter_num in existing_chapters_status:
                existing_pages = existing_chapters_status[chapter_num]["pages"]
                print(f"[AsuraComic] Chapter {chapter_num} has "
                      f"{len(existing_pages)} pages in DB")

            time.sleep(random.uniform(2, 6))
            print(f"[AsuraComic] Processing chapter {chapter_num}")

            # Table is named after the manga hash so it matches the metadata hash column
            self.db.create_page_urls_table(manga.get_id())

            pages_info = self._store_chapter_pages(
                chapter_url=chapter_url,
                manga_id=manga.get_id(),
                manga_name=manga.get_title(),
                chapter_num=chapter_num,
                existing_pages=existing_pages,
            )

            if pages_info:
                self.metrics.record_chapter(
                    worker_id=worker_id,
                    is_new=is_new_chapter,
                    is_complete=(pages_info["downloaded"] == 0),
                    total_pages=pages_info["total"],
                    downloaded_pages=pages_info["downloaded"],
                )
                self.metrics.record_pages(
                    total=pages_info["total"],
                    downloaded=pages_info["downloaded"],
                    skipped=pages_info["skipped"],
                )

    # ─────────────────────────────────────────────────────────────────────────
    # Internal page-storage helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _store_chapter_pages(
        self,
        chapter_url: str,
        manga_id: str,
        manga_name: str,
        chapter_num: str,
        existing_pages: set,
    ) -> dict:
        page_urls = self._get_chapter_page_urls(chapter_url)
        self.metrics.record_api_call("page_urls")

        if not page_urls:
            print(f"[AsuraComic] No pages found for chapter {chapter_num}")
            return None

        total_pages = len(page_urls)
        missing = [
            (idx, url)
            for idx, url in enumerate(page_urls, start=1)
            if str(idx) not in existing_pages
        ]

        if not missing:
            print(f"[AsuraComic] Chapter {chapter_num} already complete "
                  f"({total_pages} pages) – skipping")
            return {"total": total_pages, "downloaded": 0, "skipped": total_pages}

        print(f"[AsuraComic] Chapter {chapter_num}: storing "
              f"{len(missing)}/{total_pages} pages")

        threads = []
        for page_number, page_url in missing:
            t = threading.Thread(
                target=self._threaded_store_page,
                args=(manga_id, manga_name, chapter_num, page_number, page_url),
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        return {
            "total":      total_pages,
            "downloaded": len(missing),
            "skipped":    len(existing_pages),
        }

    def _threaded_store_page(
        self,
        manga_id: str,
        manga_name: str,
        chapter_num: str,
        page_number: int,
        page_url: str,
    ) -> None:
        try:
            self.db.store_page_url(
                manga_id=manga_id,
                manga_name=manga_name,
                chapter_num=chapter_num,
                page_number=page_number,
                page_url=page_url,
            )
            print(f"[AsuraComic] Stored {manga_name} ch.{chapter_num} pg.{page_number}")
        except Exception as exc:
            print(f"[AsuraComic] Failed to store page URL: {exc}")
            self.metrics.record_page_failure()
            self.metrics.record_error("db_errors")
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
#   get_requested_manga(manga_id) -> dict | None
#   set_latest_chapters(manga)    -> bool
#   download_chapters(manga)      -> None
#
# ID / Hash strategy
# ──────────────────
# manga `id` is stored as  "asura_<slug>"  (e.g. "asura_volcanic-age-0831b5e3")
# which uniquely namespaces AsuraComic entries from MangaDex entries in the
# shared manga_metadata table and keeps table-name generation identical to the
# MangaDex path (manga_id.replace("-", "_")).
#
# The `hash` column value is the manga_id string itself — deterministic and
# unique, matching the contract that SQLiteHelper expects.
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL       = "https://asuracomic.net"
SOURCE_PREFIX  = "asura_"
ITEMS_PER_PAGE = 20   # series cards per listing page

# Shared session with browser-like headers
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

def _id_from_slug(slug: str) -> str:
    return f"{SOURCE_PREFIX}{slug}"


def _slug_from_id(manga_id: str) -> str:
    return manga_id[len(SOURCE_PREFIX):]


def _normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().replace("\x00", "")


def _slug_from_url(href: str) -> str | None:
    """Extract series slug from any AsuraComic series URL."""
    m = re.search(r"/series/([^/?#]+)", href)
    return m.group(1) if m else None


# ─────────────────────────────────────────────────────────────────────────────

class AsuraComicHelper:

    def __init__(self):
        self.db      = SQLiteHelper()
        self.metrics = MetricsCollector()

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP fetch
    # ─────────────────────────────────────────────────────────────────────────

    def _get_html(self, url: str, retries: int = 4) -> str | None:
        for attempt in range(retries):
            try:
                resp = _SESSION.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response else 0
                if status == 429:
                    wait = 60 * (attempt + 1)
                    print(f"[AsuraComic] Rate-limited (429) – sleeping {wait}s")
                    self.metrics.record_error("rate_limits")
                    time.sleep(wait)
                else:
                    wait = 2 ** attempt + random.uniform(0, 2)
                    print(f"[AsuraComic] HTTP {status} on {url} (attempt {attempt+1}) "
                          f"– retrying in {wait:.1f}s")
                    self.metrics.record_error("api_errors")
                    time.sleep(wait)
            except Exception as exc:
                wait = 2 ** attempt + random.uniform(0, 2)
                print(f"[AsuraComic] Request error: {exc} – retrying in {wait:.1f}s")
                self.metrics.record_error("api_errors")
                time.sleep(wait)

        print(f"[AsuraComic] Gave up after {retries} attempts: {url}")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Listing page  –  /series?page=N
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_list_page(self, soup: BeautifulSoup) -> list[dict]:
        manga_list: list[dict] = []
        seen: set              = set()

        for anchor in soup.select("a[href*='/series/']"):
            try:
                href = anchor.get("href", "")
                # Skip chapter-level links  (/series/<slug>/chapter/N)
                if "/chapter/" in href:
                    continue

                slug = _slug_from_url(href)
                if not slug or slug in seen:
                    continue
                seen.add(slug)

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
                    "id":          _id_from_slug(slug),
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

    def _parse_detail_page(self, soup: BeautifulSoup, manga_id: str) -> dict | None:
        try:
            title_tag = (
                soup.select_one("div.text-center span.text-xl.font-bold")
                or soup.select_one("h1.text-xl")
                or soup.select_one("h1")
            )
            title = _normalize(title_tag.get_text()) if title_tag else "Unknown Title"

            cover_tag = (
                soup.select_one("img[alt='poster']")
                or soup.select_one("div.relative img")
                or soup.select_one(".series-cover img")
            )
            cover = ""
            if cover_tag:
                cover = cover_tag.get("src") or cover_tag.get("data-src") or ""

            desc_tag = (
                soup.select_one("span.font-medium.text-sm")
                or soup.select_one("div.summary__content")
                or soup.select_one("p.summary")
            )
            description = _normalize(desc_tag.get_text()) if desc_tag else ""

            genre_els = (
                soup.select("div.genres a")
                or soup.select("button.inline-flex.items-center")
                or soup.select("a[href*='/genre/']")
                or soup.select("a[href*='?genres']")
            )
            seen_tags: set = set()
            tags: list[str] = []
            for el in genre_els:
                t = _normalize(el.get_text())
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
        Returns chapters shaped identically to MangaDex feed items so that
        MangaFactory / Manga.set_chapters() receives the expected structure:
            { "id": <full_chapter_url>, "attributes": {"chapter": "<num>"} }
        """
        chapter_anchors = (
            soup.select(f"a[href*='/series/{slug}/chapter/']")
            or soup.select("a[href*='/chapter/']")
        )

        chapters: list[dict] = []
        seen_nums: set       = set()

        for a in chapter_anchors:
            href = a.get("href", "")
            m    = re.search(r"/chapter/([\d]+(?:[._-][\d]+)?)", href)
            if not m:
                continue
            # Normalise separators: "1-5" / "1_5"  ->  "1.5"
            ch_num = m.group(1).replace("_", ".").replace("-", ".")
            if ch_num in seen_nums:
                continue
            seen_nums.add(ch_num)

            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
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

        images = (
            soup.select("div#chapter-container img")
            or soup.select("div.chapter-content img")
            or soup.select("div.reading-content img")
            or soup.select("div[class*='reader'] img")
            or soup.select("main img[src*='asuracomic']")
            or soup.select("main img[src*='gg.asuracomic']")
            or soup.select("img[alt*='chapter']")
        )

        seen: set     = set()
        urls: list[str] = []
        for img in images:
            src = (
                img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            ).strip()
            if src.startswith("http") and src not in seen:
                seen.add(src)
                urls.append(src)

        # Fallback – any CDN image that looks like a page asset
        if not urls:
            for img in soup.select("img"):
                src = (img.get("src") or img.get("data-src") or "").strip()
                if ("asuracomic" in src or "gg." in src) and src.startswith("http"):
                    if src not in seen:
                        seen.add(src)
                        urls.append(src)

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

    def get_requested_manga(self, manga_id: str) -> dict | None:
        slug = _slug_from_id(manga_id)
        url  = f"{BASE_URL}/series/{slug}"

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
        Fetch chapter list, attach it to the manga object, return True if there
        are new chapters to download.  Matches MangaDexHelper contract exactly.
        """
        slug  = _slug_from_id(manga.get_id())
        url   = f"{BASE_URL}/series/{slug}"

        table_name      = manga.get_id().replace("-", "_")
        existing_latest = self.db.get_manga_latest_chapter(table_name, manga.get_id())

        time.sleep(random.uniform(1, 4))

        print(f"[AsuraComic] Fetching chapters for {manga.get_id()}")
        html = self._get_html(url)
        if not html:
            return False

        self.metrics.record_api_call("chapter_feed")
        soup     = BeautifulSoup(html, "html.parser")
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

    def download_chapters(self, manga) -> None:
        """
        Iterate chapters and store only missing pages to the DB.
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
    ) -> dict | None:
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
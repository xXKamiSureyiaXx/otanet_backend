import re
import time
import threading
import random
from bs4 import BeautifulSoup
from sqlite_helper import SQLiteHelper
from metrics_collector import MetricsCollector

# ─────────────────────────────────────────────────────────────────────────────
# NatoMangaHelper
# Uses undetected-chromedriver to bypass Cloudflare.
# The driver is passed in from main.py so it is shared across the pipeline
# and only one browser instance is ever open at a time.
#
# pyvirtualdisplay is started in main.py (Linux) so the browser window
# is hidden. On Windows the window will be visible but minimized.
#
# Public interface (mirrors MangaDexHelper):
#   get_recent_manga(offset)      -> list[dict]
#   get_requested_manga(manga_id) -> dict | None
#   set_latest_chapters(manga)    -> bool
#   download_chapters(manga)      -> None
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL       = "https://www.natomanga.com"
SOURCE_PREFIX  = "nato_"
ITEMS_PER_PAGE = 24

FEEDS = {
    "latest":    "latest-manga",
    "hot":       "hot-manga",
    "new":       "new-manga",
    "completed": "completed-manga",
}


class NatoMangaHelper:

    def __init__(self, driver, driver_lock: threading.Lock):
        """
        Args:
            driver:      A live undetected_chromedriver.Chrome instance.
            driver_lock: Threading lock — callers must hold this when the
                         driver is in use since selenium is not thread-safe.
        """
        self.driver      = driver
        self.driver_lock = driver_lock
        self.db          = SQLiteHelper()
        self.metrics     = MetricsCollector()

    # ─────────────────────────────────────────────────────────────────────────
    # Browser fetch  (all HTTP goes through here)
    # ─────────────────────────────────────────────────────────────────────────

    def _get_html(self, url: str, retries: int = 3):
        """
        Navigate to *url* and return the page source as a string.
        Detects the Cloudflare challenge page and waits it out.
        The driver_lock is acquired for the full duration of the request.
        """
        with self.driver_lock:
            for attempt in range(retries):
                try:
                    self.driver.get(url)
                    time.sleep(3)  # initial settle

                    # Wait out Cloudflare challenge if present
                    deadline = time.time() + 20
                    while "Just a moment" in self.driver.title:
                        if time.time() > deadline:
                            print(f"[NatoManga] CF challenge timed out on {url}")
                            break
                        time.sleep(2)

                    if "Just a moment" in self.driver.title:
                        print(f"[NatoManga] Still on CF page, attempt {attempt + 1}/{retries}")
                        continue

                    return self.driver.page_source

                except Exception as exc:
                    wait = 2 ** attempt + random.uniform(0, 2)
                    print(f"[NatoManga] Error ({exc}) – retrying in {wait:.1f}s")
                    self.metrics.record_error("api_errors")
                    time.sleep(wait)

        print(f"[NatoManga] Gave up after {retries} attempts: {url}")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Text helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize(text: str) -> str:
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip().replace("\x00", "")

    @staticmethod
    def _slug_to_id(slug: str) -> str:
        return f"{SOURCE_PREFIX}{slug}"

    @staticmethod
    def _id_to_slug(manga_id: str) -> str:
        return manga_id.replace(SOURCE_PREFIX, "", 1)

    @staticmethod
    def _offset_to_page(offset: int) -> int:
        return (offset // ITEMS_PER_PAGE) + 1

    # ─────────────────────────────────────────────────────────────────────────
    # List page parsing
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_list_page(self, soup: BeautifulSoup) -> list[dict]:
        manga_list = []
        seen = set()

        cards = (
            soup.select("a.list-story-item")
            or soup.select("div.list-truyen-item-wrap a[href*='/manga/']")
            or soup.select("div.itemupdate h3 a")
        )

        for el in cards:
            try:
                anchor = el if el.name == "a" else el.select_one("a[href*='/manga/']")
                if not anchor:
                    continue

                href = anchor.get("href", "")
                if "/manga/" not in href:
                    continue

                slug = href.rstrip("/").split("/manga/")[-1].split("/")[0]
                if not slug or slug in seen:
                    continue
                seen.add(slug)

                title_tag = anchor.select_one("h3") or anchor
                title = self._normalize(title_tag.get_text())

                img = anchor.select_one("img")
                cover = (img.get("src") or img.get("data-src") or "") if img else ""

                manga_list.append({
                    "id":          self._slug_to_id(slug),
                    "title":       title,
                    "description": "",
                    "cover_img":   cover,
                    "tags":        [],
                })
            except Exception as exc:
                print(f"[NatoManga] Card parse error: {exc}")

        return manga_list

    # ─────────────────────────────────────────────────────────────────────────
    # Detail page parsing
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_detail_page(self, soup: BeautifulSoup, manga_id: str) -> dict:
        try:
            title_tag = (
                soup.select_one("div.story-info-right h1")
                or soup.select_one("h1.story-info-right-extent")
                or soup.select_one("h1")
            )
            title = self._normalize(title_tag.get_text()) if title_tag else "Unknown Title"

            cover_tag = (
                soup.select_one("span.info-image img")
                or soup.select_one(".story-info-left img")
                or soup.select_one("div.manga-info-top img")
            )
            cover = cover_tag.get("src", "") if cover_tag else ""

            desc_tag = (
                soup.select_one("#panel-story-info-description")
                or soup.select_one("div.story-info-description")
            )
            description = ""
            if desc_tag:
                for el in desc_tag.select("h3, strong, label"):
                    el.decompose()
                description = self._normalize(desc_tag.get_text())

            genre_links = (
                soup.select("td.table-value a[href*='/genre/']")
                or soup.select("a.a-h[href*='/genre/']")
            )
            tags = [self._normalize(a.get_text()) for a in genre_links if a.get_text().strip()]

            return {
                "id":          manga_id,
                "title":       title,
                "description": description,
                "cover_img":   cover,
                "tags":        tags,
            }
        except Exception as exc:
            print(f"[NatoManga] Detail parse error: {exc}")
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Chapter list parsing
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_chapter_list(self, soup: BeautifulSoup) -> list[dict]:
        chapter_anchors = (
            soup.select("ul.row-content-chapter li a")
            or soup.select("div.chapter-list a[href*='/chapter-']")
            or soup.select("li.a-h a[href*='/chapter-']")
        )

        chapters = []
        for a in chapter_anchors:
            href = a.get("href", "")
            ch_slug = href.rstrip("/").split("/")[-1]
            m = re.search(r"chapter[_-]([\d]+(?:[_.-][\d]+)?)", ch_slug, re.I)
            if not m:
                continue
            ch_num = m.group(1).replace("_", ".").replace("-", ".")
            chapters.append({
                "id":         href,
                "attributes": {"chapter": ch_num},
            })

        chapters.sort(key=lambda c: float(c["attributes"]["chapter"]))
        return chapters

    # ─────────────────────────────────────────────────────────────────────────
    # Chapter page URLs
    # ─────────────────────────────────────────────────────────────────────────

    def _get_chapter_page_urls(self, chapter_url: str) -> list[str]:
        html = self._get_html(chapter_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        images = (
            soup.select("div.container-chapter-reader img")
            or soup.select("div#vungdoc img")
            or soup.select("div.panel-read-story img")
            or soup.select("img[src*='2xstorage']")
        )
        return [
            (img.get("src") or img.get("data-src") or "").strip()
            for img in images
            if (img.get("src") or img.get("data-src") or "").startswith("http")
        ]

    # ─────────────────────────────────────────────────────────────────────────
    # Public API  (mirrors MangaDexHelper)
    # ─────────────────────────────────────────────────────────────────────────

    def get_recent_manga(self, offset: int, feed: str = "latest") -> list[dict]:
        page = self._offset_to_page(offset)
        feed_path = FEEDS.get(feed, "latest-manga")
        url = f"{BASE_URL}/manga-list/{feed_path}?page={page}"

        print(f"[NatoManga] List page {page} ({feed_path})")
        html = self._get_html(url)
        if not html:
            return []

        self.metrics.record_api_call("manga_list")
        soup = BeautifulSoup(html, "html.parser")
        return self._parse_list_page(soup)

    def get_requested_manga(self, manga_id: str) -> dict:
        slug = self._id_to_slug(manga_id)
        url = f"{BASE_URL}/manga/{slug}"

        print(f"[NatoManga] Detail: {url}")
        html = self._get_html(url)
        if not html:
            self.metrics.record_error("api_errors")
            return None

        self.metrics.record_api_call("manga_list")
        soup = BeautifulSoup(html, "html.parser")
        return self._parse_detail_page(soup, manga_id)

    def set_latest_chapters(self, manga) -> bool:
        slug = self._id_to_slug(manga.get_id())
        url = f"{BASE_URL}/manga/{slug}"

        table_name = manga.get_id().replace("-", "_")
        existing_latest = self.db.get_manga_latest_chapter(table_name, manga.get_id())

        time.sleep(random.uniform(3, 8))

        print(f"[NatoManga] Fetching chapters for {manga.get_id()}")
        html = self._get_html(url)
        if not html:
            return False

        self.metrics.record_api_call("chapter_feed")
        soup = BeautifulSoup(html, "html.parser")
        chapters = self._parse_chapter_list(soup)

        if not chapters:
            print(f"[NatoManga] No chapters found for {manga.get_id()}")
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
                print(f"[NatoManga] No new chapters for {manga.get_id()} "
                      f"(latest={manga.get_latest_chapter()}, db={existing_latest})")
                return False
            print(f"[NatoManga] New chapters for {manga.get_id()} "
                  f"(latest={manga.get_latest_chapter()}, db={existing_latest})")

        return should_download

    def download_chapters(self, manga) -> None:
        existing_chapters_status = self.db.get_chapters_with_status(manga.get_id())
        print(f"[NatoManga] {len(existing_chapters_status)} existing chapters in DB "
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
                print(f"[NatoManga] Chapter {chapter_num} has "
                      f"{len(existing_pages)} pages in DB")

            time.sleep(random.uniform(4, 10))
            print(f"[NatoManga] Processing chapter {chapter_num}")

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
    # Page URL storage
    # ─────────────────────────────────────────────────────────────────────────

    def _store_chapter_pages(self, chapter_url, manga_id, manga_name,
                             chapter_num, existing_pages) -> dict:
        page_urls = self._get_chapter_page_urls(chapter_url)
        self.metrics.record_api_call("page_urls")

        if not page_urls:
            print(f"[NatoManga] No pages found for chapter {chapter_num}")
            return None

        total_pages = len(page_urls)
        missing = [
            (idx, url) for idx, url in enumerate(page_urls, start=1)
            if str(idx) not in existing_pages
        ]

        if not missing:
            print(f"[NatoManga] Chapter {chapter_num} complete ({total_pages} pages)")
            return {"total": total_pages, "downloaded": 0, "skipped": total_pages}

        print(f"[NatoManga] Chapter {chapter_num}: storing "
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

    def _threaded_store_page(self, manga_id, manga_name, chapter_num,
                             page_number, page_url) -> None:
        try:
            self.db.store_page_url(
                manga_id=manga_id,
                manga_name=manga_name,
                chapter_num=chapter_num,
                page_number=page_number,
                page_url=page_url,
            )
            print(f"[NatoManga] Stored {manga_name} ch.{chapter_num} pg.{page_number}")
        except Exception as exc:
            print(f"[NatoManga] Failed to store page URL: {exc}")
            self.metrics.record_page_failure()
            self.metrics.record_error("db_errors")
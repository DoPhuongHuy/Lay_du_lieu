#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  Multi-Platform Job Scraper — Data & IT Sector — Vietnam                    ║
║  Nền tảng hỗ trợ:                                                           ║
║    1. VietnamWorks.com                                                      ║
║    2. TopCV.vn                                                              ║
║  Tác giả: Senior Data Engineer & Web Scraping Specialist                   ║
║  Công nghệ: Python + Playwright (async) + BeautifulSoup4 + pandas          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Cách chạy:
  ▸ Demo (mở giao diện browser, 1-2 trang/platform để kiểm chứng nhanh):
      python scraper.py --demo --max-pages 2

  ▸ Thu thập đầy đủ (headless, 1200 tin, ghi tiếp nối vào raw_jobs_data.csv):
      python scraper.py --full --target 1200

  ▸ Chỉ định nền tảng cần cào (VietnamWorks trước, sau đó TopCV):
      python scraper.py --full --target 800 --platforms vietnamworks,topcv

  ▸ Tùy chỉnh từ khóa:
      python scraper.py --full --target 500 --keywords "Data Analyst,AI Engineer,Data Science"
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup
from playwright.async_api import BrowserContext, Page, async_playwright, TimeoutError as PwTimeout

# ═══════════════════════════════════════════════════════════════════════════════
#  CẤU HÌNH TOÀN CỤC
# ═══════════════════════════════════════════════════════════════════════════════
DEFAULT_KEYWORDS: list[str] = [
    "AI Engineer",
    "Machine Learning",
    "Data Analyst",
    "Data Science",
    "Data Engineer",
]

SUPPORTED_PLATFORMS = [
    "vietnamworks",
    "topcv",
]

OUTPUT_DIR       = Path(__file__).parent
CSV_FILE         = OUTPUT_DIR / "raw_jobs_data.csv"
JSON_FILE        = OUTPUT_DIR / "raw_jobs_data.json"
CHECKPOINT_FILE  = OUTPUT_DIR / ".scraper_checkpoint.json"
LOG_FILE         = OUTPUT_DIR / "scraper.log"

DELAY_RANGE      = (2.0, 5.0)
NAV_TIMEOUT      = 40_000       # ms
SELECTOR_TIMEOUT = 10_000       # ms
CHECKPOINT_EVERY = 25           # lưu mỗi N tin mới

# User-Agent pool chuẩn 100% Windows Chrome (tránh bị phát hiện mismatch OS)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
]

# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING (Ghi đồng thời ra Terminal và file scraper.log)
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("scraper")

# ═══════════════════════════════════════════════════════════════════════════════
#  SCHEMA DỮ LIỆU
# ═══════════════════════════════════════════════════════════════════════════════
FIELDNAMES = [
    "scraped_timestamp",
    "source_platform",
    "search_keyword",
    "job_url",
    "job_title",
    "company_name",
    "location",
    "salary_raw",
    "experience_required",
    "job_description",
    "requirements_and_benefits",
]


# ═══════════════════════════════════════════════════════════════════════════════
#  HÀM TIỆN ÍCH
# ═══════════════════════════════════════════════════════════════════════════════
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def human_delay(lo: float = DELAY_RANGE[0], hi: float = DELAY_RANGE[1]):
    """Chờ ngẫu nhiên mô phỏng hành vi người dùng."""
    await asyncio.sleep(random.uniform(lo, hi))


def clean(text: str | None) -> str:
    """Chuẩn hóa khoảng trắng, strip."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def pick_ua() -> str:
    """Chọn ngẫu nhiên 1 User-Agent."""
    return random.choice(USER_AGENTS)


def new_record(platform: str = "", keyword: str = "") -> dict:
    """Tạo bản ghi trống theo schema."""
    return {f: "" for f in FIELDNAMES} | {
        "scraped_timestamp": ts(),
        "source_platform": platform,
        "search_keyword": keyword,
    }


JUNK_TITLE_KEYWORDS = [
    "topcv.vn",
    "vietnamworks.com",
    "nha khoa",
    "nước thải",
    "hải quan",
    "tạp vụ",
    "phụ bếp",
    "thợ hàn",
    "bảo vệ",
    "bán lẻ / bán sỉ",
    "bất động sản",
    "bảo hiểm",
    "danh mục",
]


def is_valid_record(rec: dict | None) -> bool:
    """Kiểm tra bản ghi hợp lệ: bắt buộc có công ty, tiêu đề chuẩn, không phải tin rác/tên miền."""
    if not rec:
        return False
    title = (rec.get("job_title") or "").strip()
    title_lower = title.lower()
    comp = (rec.get("company_name") or "").strip()
    url = (rec.get("job_url") or "").strip()

    # 1. Tiêu đề không rỗng và dài >= 4 ký tự
    if not title or len(title) < 4:
        return False

    # 2. Tiêu đề không chứa domain hoặc ngành nghề ngoài lề (nha khoa, nước thải...)
    for junk in JUNK_TITLE_KEYWORDS:
        if junk in title_lower:
            return False

    # 3. Tiêu đề không phải dạng danh mục tổng hợp ("36 việc làm...", "Theo ngày cập nhật...")
    if re.search(r"^\d+[\s,.]*\d*\s*việc làm", title_lower) or "theo ngày cập nhật" in title_lower:
        return False

    # 4. Bắt buộc phải có tên công ty
    if not comp or len(comp) < 2:
        return False

    # 5. Phải có URL hợp lệ
    if not url or not url.startswith("http"):
        return False

    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  LƯU TRỮ & CHECKPOINT (Hỗ trợ nạp và ghi tiếp nối dữ liệu hiện có)
# ═══════════════════════════════════════════════════════════════════════════════
class DataStore:
    """Quản lý dữ liệu: nạp dữ liệu cũ (append mode), dedup theo URL, checkpoint, xuất CSV/JSON."""

    def __init__(self, clean_start: bool = False):
        self.records: list[dict] = []
        self._saved_urls: set[str] = set()   # Tập hợp URL của các bản ghi đã lưu thành công (dedup)
        self._seen_urls: set[str] = set()    # Tập hợp tất cả URL đã quét qua trong phiên
        self.initial_count: int = 0

        if clean_start:
            if CHECKPOINT_FILE.exists():
                CHECKPOINT_FILE.unlink()
            # Sao lưu dữ liệu cũ đề phòng người dùng cần lại
            if CSV_FILE.exists() and CSV_FILE.stat().st_size > 100:
                backup_csv = OUTPUT_DIR / f"raw_jobs_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                try:
                    import shutil
                    shutil.copy(CSV_FILE, backup_csv)
                    log.info("💾 Đã tự động sao lưu dữ liệu cũ sang: %s", backup_csv.name)
                except Exception:
                    pass
            self._init_empty_files()
            log.info("🗑  Chế độ --clean-start: Đã làm rỗng file dữ liệu và bắt đầu mới từ số 0")
        else:
            self._load_existing_data()

    def _init_empty_files(self):
        """Khởi tạo file CSV và JSON rỗng ngay lập tức khi chọn clean-start."""
        try:
            with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("⚠  Lỗi khởi tạo file rỗng: %s", e)

    def _load_existing_data(self):
        """Nạp dữ liệu đã có từ checkpoint hoặc từ file CSV/JSON hiện tại để tiếp nối."""
        # 1. Thử nạp từ checkpoint đang dở dang trước
        if CHECKPOINT_FILE.exists():
            try:
                data = json.loads(CHECKPOINT_FILE.read_text("utf-8"))
                for row in data.get("records", []):
                    if is_valid_record(row):
                        url = (row.get("job_url") or "").strip()
                        if url and url not in self._saved_urls:
                            self._saved_urls.add(url)
                            self._seen_urls.add(url)
                            self.records.append(row)
                log.info("♻  Khôi phục từ checkpoint: %d bản ghi hợp lệ", len(self.records))
            except Exception as e:
                log.warning("⚠  Lỗi đọc checkpoint: %s", e)

        # 2. Nếu không có checkpoint nhưng đã có CSV_FILE thì nạp dữ liệu từ CSV vào
        elif CSV_FILE.exists() and CSV_FILE.stat().st_size > 0:
            try:
                with open(CSV_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if is_valid_record(row):
                            url = (row.get("job_url") or "").strip()
                            if url and url not in self._saved_urls:
                                self._saved_urls.add(url)
                                self._seen_urls.add(url)
                                self.records.append(row)
                log.info("📂 Đã nạp %d bản ghi hợp lệ hiện có từ %s (tiếp nối dữ liệu cũ)", len(self.records), CSV_FILE.name)
            except Exception as e:
                log.warning("⚠  Lỗi nạp file CSV hiện tại: %s", e)

        # 3. Fallback: Nếu CSV không đọc được nhưng có JSON_FILE
        elif JSON_FILE.exists() and JSON_FILE.stat().st_size > 0:
            try:
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for row in data:
                            if is_valid_record(row):
                                url = (row.get("job_url") or "").strip()
                                if url and url not in self._saved_urls:
                                    self._saved_urls.add(url)
                                    self._seen_urls.add(url)
                                    self.records.append(row)
                        log.info("📂 Đã nạp %d bản ghi hợp lệ hiện có từ %s", len(self.records), JSON_FILE.name)
            except Exception as e:
                log.warning("⚠  Lỗi nạp file JSON hiện tại: %s", e)

        self.initial_count = len(self.records)

    def add(self, record: dict) -> bool:
        """Thêm bản ghi mới. Trả về True nếu thành công (hợp lệ và chưa có trong dữ liệu lưu)."""
        if not is_valid_record(record):
            return False
        url = (record.get("job_url") or "").strip()
        if url and url in self._saved_urls:
            return False
        if url:
            self._saved_urls.add(url)
            self._seen_urls.add(url)
        self.records.append(record)
        # Auto-checkpoint mỗi N bản ghi
        if len(self.records) % CHECKPOINT_EVERY == 0:
            self._save_checkpoint()
        return True

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def new_count(self) -> int:
        return len(self.records) - self.initial_count

    def _save_checkpoint(self):
        try:
            CHECKPOINT_FILE.write_text(
                json.dumps({
                    "records": self.records,
                    "seen_urls": list(self._seen_urls),
                    "saved_urls": list(self._saved_urls),
                }, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning("⚠  Lỗi lưu checkpoint: %s", e)

    def is_known_url(self, url: str) -> bool:
        u = (url or "").strip()
        return u in self._seen_urls or u in self._saved_urls

    def mark_seen(self, url: str):
        """Đánh dấu URL đã được duyệt qua (để không bao giờ cào lại trong phiên này)."""
        if url:
            self._seen_urls.add(url.strip())

    def save_all(self):
        """Xuất CSV + JSON bảo toàn toàn bộ dữ liệu (cũ + mới nối tiếp). Xử lý an toàn khi CSV bị khóa bởi Excel."""
        # 1. Ghi CSV (utf-8-sig mở Excel tiếng Việt chuẩn)
        try:
            with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
                for r in self.records:
                    writer.writerow({k: r.get(k, "") for k in FIELDNAMES})
            log.info("💾 CSV  → %s (Tổng: %d bản ghi, thêm mới: %d)", CSV_FILE.name, len(self.records), self.new_count)
        except PermissionError:
            log.warning("⚠ Không thể ghi vào %s vì file đang mở trong Excel/phần mềm khác!", CSV_FILE.name)
            fallback_csv = OUTPUT_DIR / f"raw_jobs_data_new_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            try:
                with open(fallback_csv, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                    writer.writeheader()
                    for r in self.records:
                        writer.writerow({k: r.get(k, "") for k in FIELDNAMES})
                log.info("💾 Đã lưu tạm sang CSV dự phòng: %s (Tổng: %d bản ghi)", fallback_csv.name, len(self.records))
            except Exception as e_fb:
                log.error("✗ Lỗi ghi file CSV dự phòng: %s", e_fb)
        except Exception as e:
            log.error("✗ Lỗi ghi file CSV: %s", e)

        # 2. Ghi JSON
        try:
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2, default=str)
            log.info("💾 JSON → %s (Tổng: %d bản ghi, thêm mới: %d)", JSON_FILE.name, len(self.records), self.new_count)
        except Exception as e:
            log.error("✗ Lỗi ghi file JSON: %s", e)

        # Xóa checkpoint tạm khi đã lưu thành công vào file chính
        if CHECKPOINT_FILE.exists():
            try:
                CHECKPOINT_FILE.unlink()
            except Exception:
                pass

    def save_incremental(self):
        """Lưu tăng dần (giữa chừng sau mỗi trang)."""
        self._save_checkpoint()
        self.save_all()


# ═══════════════════════════════════════════════════════════════════════════════
#  BASE SCRAPER CLASS
#  Quản lý tab riêng: search_page cố định, detail_page mở độc lập
# ═══════════════════════════════════════════════════════════════════════════════
class BaseScraper:
    PLATFORM: str = "Base"
    BASE: str = ""

    def __init__(self, context: BrowserContext, search_page: Page, store: DataStore, max_pages: int):
        self.context = context
        self.page = search_page
        self.store = store
        self.max_pages = max_pages

    async def fetch_detail_page_html(self, url: str) -> Optional[str]:
        """Mở tab riêng để cào chi tiết, không làm ảnh hưởng DOM của search_page."""
        detail_page = None
        try:
            detail_page = await self.context.new_page()
            await detail_page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")

            # Kiểm tra nếu trang chi tiết gặp thử thách Cloudflare Turnstile "Just a moment..."
            html = await detail_page.content()
            if "just a moment" in html.lower() or ("cloudflare" in html.lower() and "verify" in html.lower()):
                log.info("[%s] ⏳ Trang chi tiết gặp Cloudflare, chờ tự động giải mã...", self.PLATFORM)
                await human_delay(3.5, 5.0)
                html = await detail_page.content()
                if "just a moment" in html.lower():
                    log.warning("[%s] ⚠ Trang chi tiết bị Cloudflare chặn tạm thời: %s", self.PLATFORM, url)
                    return None

            try:
                await detail_page.wait_for_selector("h1, [class*='title'], [class*='company']", timeout=4000)
            except Exception:
                pass
            await human_delay(1.5, 2.5)
            html = await detail_page.content()
            return html
        except Exception as e:
            log.warning("[%s] ✗ Lỗi tải detail: %s (%s)", self.PLATFORM, url, e)
            return None
        finally:
            if detail_page:
                try:
                    await detail_page.close()
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════════════════════
#  1. TopCV.vn SCRAPER (Đã tối ưu Hybrid Card + Chốt dừng đúng số trang thực tế)
# ═══════════════════════════════════════════════════════════════════════════════
class TopCVScraper(BaseScraper):
    PLATFORM = "TopCV"
    BASE     = "https://www.topcv.vn"

    @staticmethod
    def keyword_to_slug(keyword: str) -> str:
        """Chuyển đổi từ khóa thành URL slug chính thức của TopCV (ví dụ: 'AI Engineer' -> 'ai-engineer')."""
        clean_kw = re.sub(r"[^\w\s\+\#]", " ", keyword).strip().lower()
        slug = re.sub(r"[\s\+\#]+", "-", clean_kw).strip("-")
        return slug

    @staticmethod
    def search_url(keyword: str, page: int) -> str:
        """Tạo URL tìm kiếm chuẩn xác của TopCV: /tim-viec-lam-{slug} (đảm bảo 100% khớp từ khóa)."""
        slug = TopCVScraper.keyword_to_slug(keyword)
        if page == 1:
            return f"https://www.topcv.vn/tim-viec-lam-{slug}?type_keyword=1&sba=1"
        return f"https://www.topcv.vn/tim-viec-lam-{slug}?page={page}"

    async def _search_by_keyword_ui(self, keyword: str) -> bool:
        """Tự động điền từ khóa vào ô tìm kiếm và bấm nút Tìm kiếm trên giao diện TopCV."""
        log.info("[TopCV] ⌨ Điền từ khóa '%s' vào ô tìm kiếm và bấm nút...", keyword)
        try:
            await self.page.goto("https://www.topcv.vn/viec-lam", timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(1.5, 2.5)

            input_loc = self.page.locator("#keyword, input[name='keyword'], input[placeholder*='Vị trí tuyển dụng'], #input-search-job").first
            if await input_loc.is_visible():
                await input_loc.click()
                await input_loc.fill("")
                await human_delay(0.3, 0.6)
                await input_loc.type(keyword, delay=60)
                await human_delay(0.5, 1.0)

                btn_loc = self.page.locator("button.btn.btn-topcv.btn-search-job, button.btn-search-job").first
                if await btn_loc.is_visible():
                    await btn_loc.click()
                    await human_delay(2.5, 4.0)

                cur_url = self.page.url
                if "tim-viec-lam-" in cur_url or "keyword" in cur_url:
                    log.info("[TopCV] 🔍 Kết quả tìm kiếm đã chuyển sang: %s", cur_url)
                    return True

            direct_url = self.search_url(keyword, 1)
            log.info("[TopCV] Điều hướng trực tiếp đến trang tìm kiếm chuẩn: %s", direct_url)
            await self.page.goto(direct_url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(2.0, 3.0)
            return True
        except Exception as e:
            log.warning("[TopCV] ✗ Không thể tương tác form tìm kiếm: %s, fallback dùng URL", e)
            try:
                direct_url = self.search_url(keyword, 1)
                log.info("[TopCV] Fallback điều hướng đến: %s", direct_url)
                await self.page.goto(direct_url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await human_delay(2.0, 3.0)
                return True
            except Exception:
                return False

    async def _collect_cards(self, keyword: str, page: int) -> list[dict]:
        """Thu thập đầy đủ thông tin các Job Cards trên trang tìm kiếm (Title, Company, Salary, Location, URL)."""
        if page == 1:
            ui_success = await self._search_by_keyword_ui(keyword)
            if not ui_success:
                url = self.search_url(keyword, 1)
                log.info("[TopCV] KW='%s' trang 1 → %s", keyword, url)
                try:
                    await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await human_delay(2.0, 3.5)
                except Exception as e:
                    log.warning("[TopCV] ✗ Lỗi tải trang tìm kiếm: %s", e)
                    return []
        else:
            url = self.search_url(keyword, page)
            log.info("[TopCV] KW='%s' trang %d → %s", keyword, page, url)
            try:
                await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await human_delay(2.0, 3.5)
            except Exception as e:
                log.warning("[TopCV] ✗ Lỗi tải trang tìm kiếm: %s", e)
                return []

        # Chờ phần tử công việc được render vào DOM
        for sel in [".job-item-search-result", "#job-list-search-result", ".job-item-default", "[data-job-id]"]:
            try:
                await self.page.wait_for_selector(sel, timeout=7000)
                break
            except Exception:
                continue
        await human_delay(1.5, 2.5)

        # Trích xuất toàn bộ dữ liệu thẻ Job Card qua DOM evaluate
        raw_cards = []
        try:
            raw_cards = await self.page.evaluate(r"""() => {
                const results = [];
                const items = document.querySelectorAll(
                    '#job-list-search-result .job-item-search-result, ' +
                    '.job-list-search-result .job-item-search-result, ' +
                    '.job-item-search-result:not(.suggest-job), ' +
                    '[data-job-id]'
                );
                for (const item of items) {
                    if (item.closest('.box-job-suggest') || item.closest('.suggest-job-list')) continue;
                    const a = item.querySelector('h3.title a, a.title, a[href*="/viec-lam/"]');
                    if (!a) continue;
                    const href = a.getAttribute('href') || '';
                    if (!/\/viec-lam\/[^/]+\/\d+\.html/.test(href)) continue;

                    const title = (a.innerText || a.getAttribute('title') || '').trim();
                    const compEl = item.querySelector('.company, a.company, .company-name, [class*="company"]');
                    const company = compEl ? compEl.innerText.trim() : '';
                    const salEl = item.querySelector('.salary, [class*="salary"]');
                    const salary = salEl ? salEl.innerText.trim() : 'Thương lượng';
                    const locEl = item.querySelector('.address, .city, [class*="address"], [class*="location"]');
                    const location = locEl ? locEl.innerText.trim() : 'Việt Nam';
                    const expEl = item.querySelector('.exp, [class*="exp"]');
                    const exp = expEl ? expEl.innerText.trim() : '';

                    results.push({
                        url: a.href,
                        title: title,
                        company: company,
                        salary: salary,
                        location: location,
                        experience: exp
                    });
                }
                return results;
            }""")
        except Exception:
            pass

        cards: list[dict] = []
        seen_urls_in_page = set()
        for c in raw_cards:
            full = c.get("url", "")
            if not full:
                continue
            full = full.split("?")[0]
            if not re.search(r"/viec-lam/[a-zA-Z0-9\-]+/\d+\.html", full):
                continue
            if full not in seen_urls_in_page and not self.store.is_known_url(full):
                seen_urls_in_page.add(full)
                c["url"] = full
                cards.append(c)
                if len(cards) >= 20:
                    break

        log.info("[TopCV]   → %d link tin việc làm thực tế", len(cards))
        return cards

    async def _get_total_pages(self) -> int:
        """Đọc tổng số trang kết quả thực tế từ thanh phân trang của TopCV."""
        try:
            total = await self.page.evaluate(r"""() => {
                const pageNums = [];
                // 1. Quét các thẻ số trang trong pagination
                const elements = document.querySelectorAll(
                    '.pagination li a, .pagination li, .pagination a, [class*="pagination"] a, [class*="pagination"] li'
                );
                for (const el of elements) {
                    const txt = el.innerText ? el.innerText.trim() : '';
                    if (/^\d+$/.test(txt)) {
                        pageNums.push(parseInt(txt, 10));
                    }
                }
                if (pageNums.length > 0) {
                    return Math.max(...pageNums);
                }

                // 2. Quét thông báo tổng số việc làm tìm thấy
                const bodyText = document.body ? document.body.innerText : '';
                const m = bodyText.match(/Tìm thấy\s+(\d+[\d,.]*)\s+việc làm/i);
                if (m) {
                    const totalJobs = parseInt(m[1].replace(/[,.]/g, ''), 10);
                    return Math.max(1, Math.ceil(totalJobs / 20));
                }

                return 1;
            }""")
            return max(1, total)
        except Exception:
            return 1

    async def _extract_detail(self, url: str, keyword: str) -> Optional[dict]:
        html = await self.fetch_detail_page_html(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")
        rec = new_record(self.PLATFORM, keyword)
        rec["job_url"] = url

        try:
            # 1. Tiêu đề công việc
            job_title = ""
            for sel in [
                "h1.job-detail__info--title",
                "h1.box-header-job__title",
                ".job-detail__info h1",
                "h1.job-title",
                "h1.title",
                ".header-job-info h1",
            ]:
                el = soup.select_one(sel)
                if el:
                    t = clean(el.get_text())
                    if t and "topcv.vn" not in t.lower() and len(t) >= 4:
                        job_title = t
                        break

            if not job_title:
                page_title = soup.select_one("title")
                if page_title:
                    raw_title = clean(page_title.get_text())
                    m = re.search(r"Tuyển dụng\s+(.+?)(?:\s*-\s*|\s*\|\s*TopCV)", raw_title, re.IGNORECASE)
                    if m:
                        job_title = clean(m.group(1))
                    elif " | " in raw_title and "topcv" not in raw_title.split(" | ")[0].lower():
                        job_title = clean(raw_title.split(" | ")[0])

            rec["job_title"] = job_title

            # 2. Tên công ty
            company_name = ""
            for sel in [
                ".company-name-label",
                "a.company-name",
                ".company-name",
                ".job-detail__company--information .company-name",
                ".job-detail__company--name",
                ".job-detail__company-title",
                "a.company-title",
                ".company-info h2",
                ".company-info a",
                ".employer-name",
            ]:
                el = soup.select_one(sel)
                if el:
                    c = clean(el.get_text())
                    if c and len(c) >= 2 and "topcv" not in c.lower():
                        company_name = c
                        break

            if not company_name:
                page_title = soup.select_one("title")
                if page_title:
                    raw_title = clean(page_title.get_text())
                    m = re.search(r"-\s*([^-|]+?)\s*\|\s*TopCV", raw_title, re.IGNORECASE)
                    if m:
                        company_name = clean(m.group(1))

            rec["company_name"] = company_name

            # 3. Mức lương
            sal = soup.select_one(".box-header-job__salary-and-report-salary, .job-detail__info--section-content-value, .salary, [class*='salary']")
            if sal:
                sal_text = sal.get_text("\n").strip().split("\n")[0]
                rec["salary_raw"] = clean(sal_text)

            # 4. Các section chi tiết
            sections: dict[str, str] = {}
            for item in soup.select(".box-job-information-detail-item, .job-description__item, .job-detail__body-left .section"):
                title_el = item.select_one("h2, h3, .title")
                title_text = clean(title_el.get_text()).lower() if title_el else ""
                content_text = clean(item.get_text())
                if title_text and content_text.lower().startswith(title_text):
                    content_text = content_text[len(title_text):].strip()
                if title_text:
                    sections[title_text] = content_text

            # 5. Kinh nghiệm & Địa điểm từ Overview
            overview = sections.get("tổng quan", "")
            if overview:
                exp_match = re.search(
                    r"(không yêu cầu kinh nghiệm|[\d]+ năm|[\d]+ - [\d]+ năm|fresher|junior|senior|\d+\s*năm kinh nghiệm)",
                    overview, re.IGNORECASE
                )
                rec["experience_required"] = exp_match.group(0) if exp_match else overview[:200]

            loc_section = sections.get("địa điểm và thời gian", "")
            if loc_section:
                loc_match = re.search(r"Địa điểm làm việc\s*(.+?)(?:Thời gian|Cách thức|$)", loc_section, re.DOTALL | re.IGNORECASE)
                rec["location"] = clean(loc_match.group(1)) if loc_match else clean(loc_section[:150])
            if not rec["location"]:
                addr = soup.select_one(".box-header-job__info-address span, .address, .location, .job-detail__info--section-content-value")
                rec["location"] = clean(addr.get_text()) if addr else ""

            # 6. Mô tả & Quyền lợi
            rec["job_description"] = sections.get("mô tả công việc", "")
            req = sections.get("yêu cầu ứng viên", "")
            ben = sections.get("quyền lợi ứng viên", "")
            inc = sections.get("thu nhập", "")
            parts = [p for p in [req, ben, inc] if p]
            rec["requirements_and_benefits"] = " ║ ".join(parts)

            if not rec["job_description"]:
                detail_box = soup.select_one(".box-job-information-detail, .job-detail__box--content, .job-detail__body-left")
                if detail_box:
                    rec["job_description"] = clean(detail_box.get_text())[:3000]

        except Exception as e:
            log.warning("[TopCV] ✗ Lỗi trích xuất %s: %s", url, e)
            return None

        return rec

    async def scrape_keyword(self, keyword: str, target: int) -> int:
        added = 0
        consecutive_empty_pages = 0
        total_pages = None

        for page_num in range(1, self.max_pages + 1):
            if self.store.count >= target:
                log.info("[TopCV] 🎯 Đạt mục tiêu %d", target)
                break

            cards = await self._collect_cards(keyword, page_num)
            if not cards:
                log.info("[TopCV] Hết kết quả '%s' tại trang %d", keyword, page_num)
                break

            # Xác định tổng số trang kết quả thực tế từ phân trang TopCV
            if total_pages is None:
                total_pages = await self._get_total_pages()
                log.info("[TopCV] 📄 Từ khóa '%s' có tổng cộng %d trang kết quả thực tế", keyword, total_pages)

            page_added = 0
            for card in cards:
                if self.store.count >= target:
                    return added

                link = card["url"]
                self.store.mark_seen(link)

                # Khởi tạo bản ghi từ dữ liệu Job Card đã bóc tách được trên trang tìm kiếm
                rec = new_record(self.PLATFORM, keyword)
                rec["job_url"] = link
                rec["job_title"] = clean(card.get("title", ""))
                rec["company_name"] = clean(card.get("company", ""))
                rec["salary_raw"] = clean(card.get("salary", "Thương lượng"))
                rec["location"] = clean(card.get("location", "Việt Nam"))
                rec["experience_required"] = clean(card.get("experience", ""))

                # Thử cào trang chi tiết để lấy thêm Mô tả (JD) và Yêu cầu đầy đủ
                detail_rec = await self._extract_detail(link, keyword)
                if detail_rec:
                    if detail_rec.get("job_title"):
                        rec["job_title"] = detail_rec["job_title"]
                    if detail_rec.get("company_name"):
                        rec["company_name"] = detail_rec["company_name"]
                    if detail_rec.get("salary_raw"):
                        rec["salary_raw"] = detail_rec["salary_raw"]
                    if detail_rec.get("location"):
                        rec["location"] = detail_rec["location"]
                    rec["job_description"] = detail_rec.get("job_description", "")
                    rec["requirements_and_benefits"] = detail_rec.get("requirements_and_benefits", "")
                else:
                    # Cứu cánh nếu trang chi tiết bị Cloudflare rate limit: dùng dữ liệu từ Job Card
                    if not rec.get("job_description"):
                        rec["job_description"] = f"Tuyển dụng {rec['job_title']} tại {rec['company_name']}. Địa điểm: {rec['location']}. Mức lương: {rec['salary_raw']}."

                if is_valid_record(rec) and self.store.add(rec):
                    added += 1
                    page_added += 1
                    log.info("[TopCV] ✓ #%d │ %s │ %s", self.store.count, rec["job_title"][:50], rec["company_name"][:30])

                await human_delay(2.0, 3.5)

            self.store.save_incremental()

            # 1. Dừng khi đã cào hết số trang thực tế của TopCV
            if total_pages and page_num >= total_pages:
                log.info("[TopCV] 🏁 Đã quét hết %d/%d trang thực tế cho từ khóa '%s'. Chuyển sang từ khóa tiếp theo!", page_num, total_pages, keyword)
                break

            # 2. Chốt ngắt thông minh: dừng nếu 2 trang liên tiếp không thêm được tin mới nào
            if page_added == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    log.info("[TopCV] ⏹ 2 trang liên tiếp không có tin mới cho '%s'. Chuyển từ khóa tiếp theo!", keyword)
                    break
            else:
                consecutive_empty_pages = 0

            await human_delay(2.5, 4.0)

        return added


# ═══════════════════════════════════════════════════════════════════════════════
#  2. VietnamWorks.com SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════
class VietnamWorksScraper(BaseScraper):
    PLATFORM = "VietnamWorks"
    BASE     = "https://www.vietnamworks.com"

    @staticmethod
    def search_url(keyword: str, page: int) -> str:
        return f"https://www.vietnamworks.com/viec-lam?q={quote_plus(keyword)}&page={page}"

    async def _collect_links(self, keyword: str, page: int) -> list[str]:
        # Luôn điều hướng trực tiếp bằng URL chuẩn của VietnamWorks để đảm bảo 100% hiển thị đúng việc làm của từ khóa
        url = self.search_url(keyword, page)
        log.info("[VNW] KW='%s' trang %d → %s", keyword, page, url)
        try:
            await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(1.5, 2.5)
        except Exception as e:
            log.warning("[VNW] ✗ Lỗi tải search: %s", e)
            return []

        # Chờ container danh sách việc làm xuất hiện
        try:
            await self.page.wait_for_selector("a[href*='-jd'], a[href*='-jv'], .job-item", timeout=6000)
        except Exception:
            pass

        # Cuộn trang từ từ xuống cuối để trigger lazy loading của React/Next.js cho đầy đủ tất cả ~40-50 việc làm
        try:
            for _ in range(6):
                await self.page.mouse.wheel(0, 1000)
                await asyncio.sleep(0.35)
            await human_delay(1.0, 1.8)
        except Exception:
            pass

        links: list[str] = []

        def add_link(raw_href: str):
            if not raw_href:
                return
            full = raw_href if raw_href.startswith("http") else urljoin(self.BASE, raw_href)
            clean_url = full.split("?")[0].strip()
            # Link JD của VietnamWorks chứa '-jd' hoặc '-jv'
            if ("-jd" in clean_url or "-jv" in clean_url) and not clean_url.endswith("/viec-lam"):
                if clean_url not in links and not self.store.is_known_url(clean_url):
                    links.append(clean_url)

        # 1. Trích xuất trực tiếp từ Live DOM qua JavaScript (bắt trọn cả các card được React render sau khi cuộn)
        try:
            dom_hrefs = await self.page.evaluate(r"""() => {
                const anchors = Array.from(document.querySelectorAll('a[href*="-jd"], a[href*="-jv"]'));
                return anchors.map(a => a.href).filter(Boolean);
            }""")
            for h in dom_hrefs:
                add_link(h)
        except Exception as e:
            log.debug("[VNW] evaluate live DOM links: %s", e)

        # 2. Bổ sung từ BeautifulSoup HTML content (đảm bảo không sót bất kỳ link nào từ VIP đến thường)
        try:
            html = await self.page.content()
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select("a[href*='-jd'], a[href*='-jv'], .job-item a[href], h2 a[href]"):
                add_link(a.get("href", ""))
        except Exception as e:
            log.debug("[VNW] parse HTML content links: %s", e)

        log.info("[VNW]   → %d link mới", len(links))
        return links

    async def _extract_detail(self, url: str, keyword: str) -> Optional[dict]:
        html = await self.fetch_detail_page_html(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")
        rec = new_record(self.PLATFORM, keyword)
        rec["job_url"] = url

        try:
            h1 = soup.select_one("h1")
            rec["job_title"] = clean(h1.get_text()) if h1 else ""

            # 1. Trích xuất tên công ty từ DOM
            for sel in [
                "a[href*='/nha-tuyen-dung/']",
                "a[href*='/companies/']",
                ".company-name",
                "[class*='company-name']",
                "[class*='companyName']",
                "[class*='employer-name']",
                "[class*='companyTitle']",
                "h2.company-name",
                ".company-title",
            ]:
                el = soup.select_one(sel)
                if el and clean(el.get_text()) and len(clean(el.get_text())) >= 2:
                    c_text = clean(el.get_text())
                    if "vietnamworks" not in c_text.lower():
                        rec["company_name"] = c_text
                        break

            # Fallback lấy tên công ty từ thẻ <title> nếu DOM không có
            page_title = soup.select_one("title")
            raw_title = clean(page_title.get_text()) if page_title else ""
            if not rec["company_name"] and raw_title:
                comp_match = re.search(r"(?:tại|at)\s+(.+?)(?:\s+T\d+/\d+|\s*\|\s*Vietnamworks|$)", raw_title, re.IGNORECASE)
                if comp_match:
                    rec["company_name"] = clean(comp_match.group(1))

            body_text = soup.get_text()
            if not rec["company_name"]:
                m = re.search(r"(Công [Tt]y\s+.{5,80}?)(?:\n|Ho Chi|Hà Nội|Hồ Chí)", body_text)
                if m:
                    rec["company_name"] = clean(m.group(1))

            # Nếu chưa có job_title thì lấy từ <title>
            if not rec["job_title"] and raw_title:
                m_title = re.search(r"^(?:Tuyển(?:\s+dụng)?\s+)?(.+?)(?:\s+(?:tại|at)\s+|\s*\|\s*Vietnamworks|$)", raw_title, re.IGNORECASE)
                if m_title:
                    rec["job_title"] = clean(m_title.group(1))

            sal_match = re.search(r"(Negotiable|Thương lượng|\$[\d,]+\s*-\s*\$[\d,]+|\d+[\s,]*-[\s,]*\d+\s*(?:triệu|USD))", body_text, re.IGNORECASE)
            rec["salary_raw"] = clean(sal_match.group(0)) if sal_match else "Thương lượng"

            loc_match = re.search(r"(Ho Chi Minh|Hà Nội|Hồ Chí Minh|Da Nang|Đà Nẵng|Binh Duong)", body_text, re.IGNORECASE)
            rec["location"] = loc_match.group(1) if loc_match else "Việt Nam"

            exp_match = re.search(r"(\d+\s*-\s*\d+\s*(?:years?|năm)|\d+\+?\s*years?|Fresher|Junior|Senior)", body_text, re.IGNORECASE)
            rec["experience_required"] = exp_match.group(0) if exp_match else ""

            main_content = soup.select_one("main, [role='main'], body")
            rec["job_description"] = clean(main_content.get_text())[:3000] if main_content else ""

        except Exception as e:
            log.warning("[VNW] ✗ Lỗi trích xuất %s: %s", url, e)
            return None

        if not is_valid_record(rec):
            log.debug("[VNW] ✗ Bỏ qua tin không hợp lệ: title='%s', company='%s'", rec["job_title"], rec["company_name"])
            return None
        return rec

    async def _get_total_pages(self) -> int:
        """Đọc tổng số trang hoặc tổng số việc làm từ VietnamWorks để ước lượng số trang."""
        try:
            total = await self.page.evaluate(r"""() => {
                // 1. Thử đọc tổng số việc làm (VD: "128 việc làm", "Tìm thấy 105 việc làm")
                const bodyText = document.body ? document.body.innerText : '';
                const mTotal = bodyText.match(/(\d+[\d,.]*)\s+việc làm/i);
                if (mTotal) {
                    const totalJobs = parseInt(mTotal[1].replace(/[^\d]/g, ''), 10);
                    if (totalJobs > 0) {
                        return Math.max(1, Math.ceil(totalJobs / 25));
                    }
                }

                // 2. Đọc các số hiển thị trên thanh phân trang
                const pageNums = [];
                const elements = document.querySelectorAll('.pagination button, .pagination li, .pagination a, [class*="pagination"] *, nav[aria-label*="pagination"] *');
                for (const el of elements) {
                    const txt = el.innerText ? el.innerText.trim() : '';
                    if (/^\d+$/.test(txt)) {
                        pageNums.push(parseInt(txt, 10));
                    }
                }
                const m = bodyText.match(/(?:trang|page)\s+\d+\s*[\/\-]\s*(\d+)/i);
                if (m) {
                    pageNums.push(parseInt(m[1], 10));
                }
                return pageNums.length > 0 ? Math.max(...pageNums) : 1;
            }""")
            return max(1, total)
        except Exception:
            return 1

    async def _has_next_page(self, current_page: int) -> bool:
        """Kiểm tra xem VietnamWorks còn trang tiếp theo để cào không."""
        try:
            return await self.page.evaluate(r"""(cur) => {
                // 1. Kiểm tra nút Next / Kế tiếp
                const nextBtn = document.querySelector('.pagination .next:not(.disabled), [class*="pagination"] [class*="next"]:not([class*="disabled"]), button[aria-label*="Next"]:not([disabled]), a[aria-label*="Next"]:not([disabled])');
                if (nextBtn) return true;

                // 2. Kiểm tra xem có số trang nào lớn hơn trang hiện tại không
                const elements = document.querySelectorAll('.pagination button, .pagination li, .pagination a, [class*="pagination"] *');
                for (const el of elements) {
                    const txt = el.innerText ? el.innerText.trim() : '';
                    if (/^\d+$/.test(txt)) {
                        const num = parseInt(txt, 10);
                        if (num > cur) return true;
                    }
                }

                // 3. Nếu có dấu ba chấm '...' trong phân trang chứng tỏ còn các trang lớn hơn chưa hiển thị
                const paginationEl = document.querySelector('.pagination, [class*="pagination"]');
                if (paginationEl && paginationEl.innerText.includes('...')) {
                    return true;
                }

                return false;
            }""", current_page)
        except Exception:
            return True

    async def scrape_keyword(self, keyword: str, target: int) -> int:
        added = 0
        consecutive_empty_pages = 0
        est_pages = None

        for page_num in range(1, self.max_pages + 1):
            if self.store.count >= target:
                log.info("[VNW] 🎯 Đạt mục tiêu %d", target)
                break

            links = await self._collect_links(keyword, page_num)
            if not links:
                log.info("[VNW] Hết kết quả '%s' tại trang %d (không còn link mới)", keyword, page_num)
                break

            # Ước lượng tổng số trang (để log hiển thị cho người dùng tiện theo dõi)
            if est_pages is None:
                est_pages = await self._get_total_pages()
                log.info("[VNW] 📄 Từ khóa '%s' ước lượng khoảng %d trang kết quả", keyword, est_pages)

            page_added = 0
            for link in links:
                if self.store.count >= target:
                    return added
                self.store.mark_seen(link)
                rec = await self._extract_detail(link, keyword)
                if rec and self.store.add(rec):
                    added += 1
                    page_added += 1
                    log.info("[VNW] ✓ #%d │ %s │ %s", self.store.count, rec["job_title"][:50], rec["company_name"][:30])
                await human_delay(2.0, 3.5)

            self.store.save_incremental()

            # 1. Kiểm tra xem có còn trang tiếp theo không (dựa vào nút Next / chỉ số trang DOM)
            has_next = await self._has_next_page(page_num)
            if not has_next and est_pages and page_num >= est_pages:
                log.info("[VNW] 🏁 Đã đến trang cuối cùng (%d trang) cho từ khóa '%s'. Chuyển sang từ khóa tiếp theo!", page_num, keyword)
                break

            # 2. Chốt ngắt thông minh: dừng nếu 2 trang liên tiếp không thêm được tin mới nào (do trùng lặp)
            if page_added == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    log.info("[VNW] ⏹ 2 trang liên tiếp không có thêm tin mới nào cho '%s'. Tự động chuyển từ khóa tiếp theo!", keyword)
                    break
            else:
                consecutive_empty_pages = 0

            await human_delay(2.5, 4.0)

        return added


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI & MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="🔍 Multi-Platform Job Scraper — Tuyển dụng Data/IT tại Việt Nam",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ sử dụng:
  python scraper.py --demo --max-pages 1
  python scraper.py --full --target 1200
  python scraper.py --full --target 800 --platforms topcv,vietnamworks
  python scraper.py --full --target 500 --keywords "AI Engineer,Data Science"
        """,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--demo", action="store_true",
        help="Chế độ demo: headless=False (hiện browser), in log chi tiết",
    )
    mode.add_argument(
        "--full", action="store_true",
        help="Chế độ thu thập đầy đủ: headless=True (chạy ngầm hoàn toàn)",
    )
    parser.add_argument(
        "--target", type=int, default=1000,
        help="Mục tiêu số lượng tin tổng cộng (mặc định: 1000)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=30,
        help="Số trang tối đa mỗi từ khóa trên mỗi nền tảng (mặc định: 30)",
    )
    parser.add_argument(
        "--keywords", type=str, default=None,
        help="Từ khóa tùy chỉnh, phân cách bằng dấu phẩy",
    )
    parser.add_argument(
        "--platforms", type=str, default=None,
        help=f"Danh sách nền tảng cào ({', '.join(SUPPORTED_PLATFORMS)}). Mặc định: tất cả",
    )
    parser.add_argument(
        "--clean-start", action="store_true",
        help="Xóa checkpoint cũ, bắt đầu thu thập mới hoàn toàn",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    headless = not args.demo
    target = args.target
    max_pages = args.max_pages
    keywords = (
        [k.strip() for k in args.keywords.split(",") if k.strip()]
        if args.keywords
        else DEFAULT_KEYWORDS
    )

    # Lựa chọn danh sách nền tảng
    if args.platforms:
        selected_platforms = [p.strip().lower() for p in args.platforms.split(",") if p.strip().lower() in SUPPORTED_PLATFORMS]
        if not selected_platforms:
            selected_platforms = SUPPORTED_PLATFORMS
    else:
        selected_platforms = SUPPORTED_PLATFORMS

    store = DataStore(clean_start=args.clean_start)

    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  Multi-Platform Job Scraper — Data & IT — Vietnam           ║")
    log.info("╠══════════════════════════════════════════════════════════════╣")
    log.info("║  Chế độ     : %s", "DEMO (headless=False)" if args.demo else "FULL (headless=True)")
    log.info("║  Mục tiêu   : %d tin (Hiện có: %d bản ghi trong file)", target, store.count)
    log.info("║  Max pages  : %d/từ khóa", max_pages)
    log.info("║  Nền tảng   : %s", ", ".join(selected_platforms))
    log.info("║  Từ khóa    : %s", ", ".join(keywords))
    log.info("║  Output     : %s (Ghi tiếp nối / Append mode)", CSV_FILE)
    log.info("╚══════════════════════════════════════════════════════════════╝")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=pick_ua(),
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
        )
        # Loại bỏ cờ webdriver để vượt qua các bộ lọc bot tự động
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)
        # Chặn hình ảnh và media nặng để tăng tốc, nhưng không chặn asset của Cloudflare/Turnstile
        async def block_heavy_media(route):
            req_url = route.request.url.lower()
            if "cloudflare" in req_url or "challenge" in req_url or "turnstile" in req_url:
                await route.continue_()
            elif re.search(r"\.(png|jpg|jpeg|gif|webp|mp4|webm)(\?.*)?$", req_url):
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", block_heavy_media)

        search_page = await context.new_page()

        # Ánh xạ class scraper (VietnamWorks chạy trước TopCV)
        scraper_classes = {
            "vietnamworks": VietnamWorksScraper,
            "topcv": TopCVScraper,
        }

        for plat_name in selected_platforms:
            if store.count >= target:
                log.info("🎯 Đã đạt mục tiêu %d tin tuyển dụng! Dừng sớm.", target)
                break

            cls = scraper_classes.get(plat_name)
            if not cls:
                continue

            log.info("\n▶▶▶ KHỞI ĐỘNG NỀN TẢNG: %s ◀◀◀", plat_name.upper())
            scraper_instance = cls(context, search_page, store, max_pages)

            for kw in keywords:
                if store.count >= target:
                    break
                count = await scraper_instance.scrape_keyword(kw, target)
                log.info("[%s] KW='%s' → %d tin mới │ Tổng toàn hệ thống: %d", plat_name.upper(), kw, count, store.count)

        await browser.close()

    # Lưu cuối cùng
    store.save_all()

    log.info("\n╔══════════════════════════════════════════════════════════════╗")
    log.info("║  ✅  HOÀN TẤT THU THẬP DỮ LIỆU                              ║")
    log.info("║  Tổng bản ghi : %-6d                                      ║", store.count)
    log.info("║  - Đã có sẵn  : %-6d                                      ║", store.initial_count)
    log.info("║  - Cào mới    : %-6d                                      ║", store.new_count)
    log.info("║  CSV          : %s", CSV_FILE.name)
    log.info("║  JSON         : %s", JSON_FILE.name)
    log.info("╚══════════════════════════════════════════════════════════════╝")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("⛔  Dừng bởi người dùng (Ctrl+C)")
        sys.exit(0)

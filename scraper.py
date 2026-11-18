#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  Job Scraper — AI / ML / Data Science — Việt Nam                           ║
║  Nguồn: TopCV.vn + VietnamWorks.com                                        ║
║  Tác giả: Senior Data Engineer & Web Scraping Specialist                   ║
║  Công nghệ: Python + Playwright (async) + BeautifulSoup4 + pandas          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Cách chạy:
  ▸ Demo (có giao diện, 2 trang):
      python scraper.py --demo --max-pages 2

  ▸ Thu thập đầy đủ (headless, 1200 tin):
      python scraper.py --full --target 1200

  ▸ Tùy chỉnh:
      python scraper.py --full --target 800 --keywords "AI Engineer,LLM,NLP"
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
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ═══════════════════════════════════════════════════════════════════════════════
#  CẤU HÌNH TOÀN CỤC
# ═══════════════════════════════════════════════════════════════════════════════
DEFAULT_KEYWORDS: list[str] = [
    "AI Engineer",
    "Machine Learning",
    "Data Analyst",
    "Data Science",
    "Python",
    "SQL",
    "PyTorch",
    "LLM",
]

OUTPUT_DIR       = Path(__file__).parent
CSV_FILE         = OUTPUT_DIR / "raw_jobs_data.csv"
JSON_FILE        = OUTPUT_DIR / "raw_jobs_data.json"
CHECKPOINT_FILE  = OUTPUT_DIR / ".scraper_checkpoint.json"
LOG_FILE         = OUTPUT_DIR / "scraper.log"

DELAY_RANGE      = (2.0, 5.0)
NAV_TIMEOUT      = 40_000       # ms
SELECTOR_TIMEOUT = 10_000       # ms
CHECKPOINT_EVERY = 25           # lưu mỗi N tin

# User-Agent rotation pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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


# ═══════════════════════════════════════════════════════════════════════════════
#  LƯU TRỮ & CHECKPOINT (Hỗ trợ nạp và ghi tiếp nối dữ liệu hiện có)
# ═══════════════════════════════════════════════════════════════════════════════
class DataStore:
    """Quản lý dữ liệu: nạp dữ liệu cũ (append mode), dedup theo URL, checkpoint, xuất CSV/JSON."""

    def __init__(self, clean_start: bool = False):
        self.records: list[dict] = []
        self._seen_urls: set[str] = set()
        self.initial_count: int = 0

        if clean_start:
            if CHECKPOINT_FILE.exists():
                CHECKPOINT_FILE.unlink()
            log.info("🗑  Chế độ --clean-start: Bắt đầu thu thập mới hoàn toàn")
        else:
            self._load_existing_data()

    def _load_existing_data(self):
        """Nạp dữ liệu đã có từ checkpoint hoặc từ file CSV/JSON hiện tại để tiếp nối."""
        # 1. Thử nạp từ checkpoint đang dở dang trước
        if CHECKPOINT_FILE.exists():
            try:
                data = json.loads(CHECKPOINT_FILE.read_text("utf-8"))
                self.records = data.get("records", [])
                self._seen_urls = set(data.get("seen_urls", []))
                log.info("♻  Khôi phục từ checkpoint: %d bản ghi", len(self.records))
            except Exception as e:
                log.warning("⚠  Lỗi đọc checkpoint: %s", e)

        # 2. Nếu không có checkpoint nhưng đã có CSV_FILE thì nạp dữ liệu từ CSV vào
        elif CSV_FILE.exists() and CSV_FILE.stat().st_size > 0:
            try:
                with open(CSV_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = (row.get("job_url") or "").strip()
                        if url:
                            if url not in self._seen_urls:
                                self._seen_urls.add(url)
                                self.records.append(row)
                        elif any(row.values()):
                            self.records.append(row)
                log.info("📂 Đã nạp %d bản ghi hiện có từ %s (tiếp nối dữ liệu cũ)", len(self.records), CSV_FILE.name)
            except Exception as e:
                log.warning("⚠  Lỗi nạp file CSV hiện tại: %s", e)

        # 3. Fallback: Nếu CSV không đọc được nhưng có JSON_FILE
        elif JSON_FILE.exists() and JSON_FILE.stat().st_size > 0:
            try:
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for row in data:
                            url = (row.get("job_url") or "").strip()
                            if url and url not in self._seen_urls:
                                self._seen_urls.add(url)
                                self.records.append(row)
                        log.info("📂 Đã nạp %d bản ghi hiện có từ %s", len(self.records), JSON_FILE.name)
            except Exception as e:
                log.warning("⚠  Lỗi nạp file JSON hiện tại: %s", e)

        self.initial_count = len(self.records)

    def add(self, record: dict) -> bool:
        """Thêm bản ghi mới. Trả về True nếu thành công (không trùng)."""
        url = record.get("job_url", "")
        if url and url in self._seen_urls:
            return False
        if url:
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
                }, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning("⚠  Lỗi lưu checkpoint: %s", e)

    def is_known_url(self, url: str) -> bool:
        return url in self._seen_urls

    def save_all(self):
        """Xuất CSV + JSON bảo toàn toàn bộ dữ liệu (cũ + mới nối tiếp)."""
        # CSV (mã hóa utf-8-sig mở Excel không lỗi font)
        with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            for r in self.records:
                writer.writerow({k: r.get(k, "") for k in FIELDNAMES})
        log.info("💾 CSV  → %s (Tổng: %d bản ghi, thêm mới: %d)", CSV_FILE, len(self.records), self.new_count)

        # JSON
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(self.records, f, ensure_ascii=False, indent=2, default=str)
        log.info("💾 JSON → %s (Tổng: %d bản ghi, thêm mới: %d)", JSON_FILE, len(self.records), self.new_count)

        # Xóa checkpoint tạm khi đã lưu thành công vào file chính
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()

    def save_incremental(self):
        """Lưu tăng dần (giữa chừng sau mỗi trang)."""
        self._save_checkpoint()
        self.save_all()


# ═══════════════════════════════════════════════════════════════════════════════
#  TopCV.vn SCRAPER
#  Selector đã xác minh ngày 2026-11-18
# ═══════════════════════════════════════════════════════════════════════════════
class TopCVScraper:
    """
    Cấu trúc TopCV (xác minh thực tế):
    ───────────────────────────────────
    Search page: /viec-lam?keyword=...&page=N
      • Job card    : div[data-job-id] > .feature-job-item
      • Title link  : a.title[href]           → href = full URL .html
      • Company     : .company-name           → innerText
      • Salary      : .salary                 → "15Tr - 30Tr"
      • Location    : .address                → "Hà Nội"
      • Pagination  : p.slick-pagination      → "1 / 85 trang"
      • Next button : i.category-level-1__header-pagination-action-icon.next

    Detail page: /viec-lam/{slug}/{id}.html
      • Title    : h1.box-header-job__title
      • Company  : .company-name-label
      • Salary   : .box-header-job__salary-and-report-salary
      • Sections : .box-job-information-detail-item
                   → h2.box-job-information-detail-item__title--title
                   Tiêu đề sections: "Tổng quan", "Mô tả công việc",
                   "Yêu cầu ứng viên", "Quyền lợi ứng viên", etc.
      • Overview : .box-job-information-detail  → innerText (fallback)
    """

    PLATFORM = "TopCV"
    BASE     = "https://www.topcv.vn"

    def __init__(self, page, store: DataStore, max_pages: int):
        self.page = page
        self.store = store
        self.max_pages = max_pages

    @staticmethod
    def search_url(keyword: str, page: int) -> str:
        return f"https://www.topcv.vn/viec-lam?keyword={quote_plus(keyword)}&page={page}"

    # ── Thu thập link job từ 1 trang kết quả ─────────────────────────────
    async def _collect_links(self, keyword: str, page: int) -> list[str]:
        url = self.search_url(keyword, page)
        log.info("[TopCV] KW='%s' trang %d → %s", keyword, page, url)

        try:
            await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay()
        except Exception as e:
            log.warning("[TopCV] ✗ Lỗi tải trang: %s", e)
            return []

        # Lấy HTML và parse bằng BeautifulSoup
        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")

        links: list[str] = []

        # Ưu tiên: a.title[href] chứa /viec-lam/
        for a in soup.select("a.title[href]"):
            href = a.get("href", "")
            if "/viec-lam/" in href and href.endswith(".html") or "/viec-lam/" in href:
                full = href if href.startswith("http") else urljoin(self.BASE, href)
                # Bỏ query params tracking
                full = full.split("?")[0]
                if full not in links and not self.store.is_known_url(full):
                    links.append(full)

        # Fallback: mọi link chứa /viec-lam/ và .html
        if not links:
            for a in soup.select("a[href*='/viec-lam/']"):
                href = a.get("href", "")
                if ".html" in href:
                    full = href if href.startswith("http") else urljoin(self.BASE, href)
                    full = full.split("?")[0]
                    if full not in links and not self.store.is_known_url(full):
                        links.append(full)

        log.info("[TopCV]   → %d link mới", len(links))
        return links

    # ── Kiểm tra trang tiếp theo ─────────────────────────────────────────
    async def _has_next(self) -> bool:
        try:
            html = await self.page.content()
            soup = BeautifulSoup(html, "lxml")
            # Tìm nút next trong pagination
            nxt = soup.select_one(
                "i.category-level-1__header-pagination-action-icon.next"
            )
            if nxt and "inactive" not in (nxt.get("class") or []):
                return True
            # Fallback: slick-pagination "X / Y trang"
            pag = soup.select_one("p.slick-pagination")
            if pag:
                text = pag.get_text()
                match = re.search(r"(\d+)\s*/\s*(\d+)", text)
                if match:
                    current, total = int(match.group(1)), int(match.group(2))
                    return current < total
        except Exception:
            pass
        return False

    # ── Trích xuất chi tiết 1 tin ─────────────────────────────────────────
    async def _extract_detail(self, url: str, keyword: str) -> Optional[dict]:
        try:
            await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(2.0, 4.0)
        except Exception as e:
            log.warning("[TopCV] ✗ Lỗi tải detail: %s", e)
            return None

        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")
        rec = new_record(self.PLATFORM, keyword)
        rec["job_url"] = url

        try:
            # Tiêu đề
            h1 = soup.select_one("h1.box-header-job__title") or soup.select_one("h1")
            rec["job_title"] = clean(h1.get_text()) if h1 else ""

            # Công ty
            comp = soup.select_one(".company-name-label") or soup.select_one(".company-name")
            rec["company_name"] = clean(comp.get_text()) if comp else ""

            # Lương
            sal = soup.select_one(".box-header-job__salary-and-report-salary")
            if sal:
                # Chỉ lấy dòng đầu (bỏ "Xem mức lương thị trường...")
                sal_text = sal.get_text("\n").strip().split("\n")[0]
                rec["salary_raw"] = clean(sal_text)

            # Phân tích các section chi tiết
            sections: dict[str, str] = {}
            for item in soup.select(".box-job-information-detail-item"):
                title_el = item.select_one(
                    "h2.box-job-information-detail-item__title--title, h2"
                )
                title_text = clean(title_el.get_text()) if title_el else ""
                # Nội dung = toàn bộ text của item trừ title
                content_text = clean(item.get_text())
                if title_text and content_text.startswith(title_text):
                    content_text = content_text[len(title_text):].strip()
                if title_text:
                    sections[title_text.lower()] = content_text

            # Ánh xạ sections → fields
            # Tổng quan → experience
            overview = sections.get("tổng quan", "")
            if overview:
                # Trích kinh nghiệm từ overview
                exp_match = re.search(
                    r"(không yêu cầu kinh nghiệm|[\d]+ năm|[\d]+ - [\d]+ năm|"
                    r"trên \d+ năm|dưới \d+ năm|fresher|junior|senior|"
                    r"\d+\s*năm kinh nghiệm)", overview, re.IGNORECASE
                )
                rec["experience_required"] = exp_match.group(0) if exp_match else overview[:200]

            # Location từ section "Địa điểm và thời gian" hoặc fallback
            loc_section = sections.get("địa điểm và thời gian", "")
            if loc_section:
                # Lấy dòng sau "Địa điểm làm việc"
                loc_match = re.search(r"Địa điểm làm việc\s*(.+?)(?:Thời gian|Cách thức|$)",
                                      loc_section, re.DOTALL | re.IGNORECASE)
                rec["location"] = clean(loc_match.group(1)) if loc_match else clean(loc_section[:150])
            if not rec["location"]:
                # Fallback: address từ header
                addr = soup.select_one(".box-header-job__info-address span")
                rec["location"] = clean(addr.get_text()) if addr else ""

            # Mô tả công việc
            rec["job_description"] = sections.get("mô tả công việc", "")

            # Yêu cầu + Quyền lợi
            req = sections.get("yêu cầu ứng viên", "")
            ben = sections.get("quyền lợi ứng viên", "")
            inc = sections.get("thu nhập", "")
            parts = [p for p in [req, ben, inc] if p]
            rec["requirements_and_benefits"] = " ║ ".join(parts)

            # Fallback: nếu không tách được, lấy overview text
            if not rec["job_description"]:
                detail_box = soup.select_one(".box-job-information-detail")
                if detail_box:
                    rec["job_description"] = clean(detail_box.get_text())[:3000]

        except Exception as e:
            log.warning("[TopCV] ✗ Lỗi trích xuất %s: %s", url, e)
            return None

        return rec if rec["job_title"] else None

    # ── Vòng lặp chính cho 1 từ khóa ─────────────────────────────────────
    async def scrape_keyword(self, keyword: str, target: int) -> int:
        """Trả về số tin mới thu được."""
        added = 0
        for page_num in range(1, self.max_pages + 1):
            if self.store.count >= target:
                log.info("[TopCV] 🎯 Đã đạt mục tiêu %d → dừng", target)
                return added

            links = await self._collect_links(keyword, page_num)
            if not links:
                log.info("[TopCV] Hết kết quả '%s' trang %d", keyword, page_num)
                break

            for link in links:
                if self.store.count >= target:
                    return added
                rec = await self._extract_detail(link, keyword)
                if rec and self.store.add(rec):
                    added += 1
                    log.info(
                        "[TopCV] ✓ #%d │ %s │ %s",
                        self.store.count,
                        rec["job_title"][:55],
                        rec["company_name"][:35],
                    )

            # Kiểm tra trang tiếp
            if not await self._has_next():
                log.info("[TopCV] Hết phân trang cho '%s'", keyword)
                break

            # Lưu tăng dần sau mỗi trang
            self.store.save_incremental()

        return added


# ═══════════════════════════════════════════════════════════════════════════════
#  VietnamWorks.com SCRAPER
#  Selector đã xác minh ngày 2026-11-18
# ═══════════════════════════════════════════════════════════════════════════════
class VietnamWorksScraper:
    """
    Cấu trúc VietnamWorks (xác minh thực tế):
    ──────────────────────────────────────────
    Search page: /viec-lam?q={keyword}  (pagination button-based, React SPA)
      • Job card     : div.job-item (class "sc-ff8b57fd-0 ... job-item")
      • Special offer: a[href*="-jd?"]  (title attr có tên job)
      • Regular job  : h2 a[href*="-jv"] (innerText có prefix "Mới")
      • Company      : text node bên trong card
      • Location     : text node bên trong card  ("Hồ Chí Minh", etc.)
      • Salary       : text node  ("Thương lượng", "Negotiable", etc.)
      • Pagination   : .pagination > li.page-item > button (click-based)
      • 46 items/page (5 special + ~41 regular, nhưng regular có placeholder)

    Detail page: /{slug}-{id}-jd hoặc -jv
      • Title       : h1  (class hashed "sc-d1ecfd52-0 ...")
      • Company     : text near logo
      • H2 sections : "Job description", "Job requirements",
                       "What We Can Offer", "Job Information", "Job Locations"
      • Info meta   : "Negotiable", "Ho Chi Minh", "Sat & Sun off", etc.
                       (all in body text, not structured tags)
    """

    PLATFORM = "VietnamWorks"
    BASE     = "https://www.vietnamworks.com"

    def __init__(self, page, store: DataStore, max_pages: int):
        self.page = page
        self.store = store
        self.max_pages = max_pages

    @staticmethod
    def search_url(keyword: str) -> str:
        return f"https://www.vietnamworks.com/viec-lam?q={quote_plus(keyword)}"

    # ── Thu thập link job từ 1 trang kết quả ─────────────────────────────
    async def _collect_links_from_current_page(self) -> list[str]:
        """Trích link từ trang hiện tại (đã navigate sẵn)."""
        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []

        # Job items: lấy link từ cả special offers (-jd) và regular (-jv)
        for item in soup.select(".job-item"):
            for a in item.select("a[href]"):
                href = a.get("href", "")
                if "-jd" in href or "-jv" in href:
                    full = href if href.startswith("http") else urljoin(self.BASE, href)
                    # Bỏ query params
                    full = full.split("?")[0]
                    if full not in links and not self.store.is_known_url(full):
                        links.append(full)
                    break  # 1 link per card

        # Fallback: h2 a links
        if not links:
            for a in soup.select("h2 a[href]"):
                href = a.get("href", "")
                if "-jv" in href or "-jd" in href:
                    full = href if href.startswith("http") else urljoin(self.BASE, href)
                    full = full.split("?")[0]
                    if full not in links and not self.store.is_known_url(full):
                        links.append(full)

        return links

    # ── Click sang trang tiếp theo (button-based pagination) ──────────────
    async def _go_to_next_page(self) -> bool:
        """Click nút trang tiếp. Trả về True nếu thành công."""
        try:
            # Tìm page-item không active, tiếp theo active
            pagination_items = await self.page.query_selector_all(".pagination .page-item")
            found_active = False
            for item in pagination_items:
                cls = await item.get_attribute("class") or ""
                if "active" in cls:
                    found_active = True
                    continue
                if found_active:
                    # Đây là nút trang tiếp
                    btn = await item.query_selector("button")
                    if btn:
                        await btn.click()
                        await human_delay(3.0, 5.0)
                        # Chờ content update
                        await self.page.wait_for_timeout(2000)
                        return True
                    break
            return False
        except Exception as e:
            log.warning("[VNW] ✗ Lỗi chuyển trang: %s", e)
            return False

    # ── Trích xuất chi tiết 1 tin (parse từ body text) ────────────────────
    async def _extract_detail(self, url: str, keyword: str) -> Optional[dict]:
        try:
            await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(2.0, 4.0)
        except Exception as e:
            log.warning("[VNW] ✗ Lỗi tải detail: %s", e)
            return None

        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")
        rec = new_record(self.PLATFORM, keyword)
        rec["job_url"] = url

        try:
            # Title
            h1 = soup.select_one("h1")
            rec["job_title"] = clean(h1.get_text()) if h1 else ""

            # Toàn bộ headings h2 → chia sections
            h2_elements = soup.select("h2")
            sections: dict[str, str] = {}
            for i, h2 in enumerate(h2_elements):
                heading = clean(h2.get_text()).lower()
                # Thu thập text giữa h2 này và h2 tiếp theo
                content_parts = []
                sibling = h2.find_next_sibling()
                while sibling:
                    if sibling.name and sibling.name in ("h2", "h1"):
                        break
                    text = clean(sibling.get_text())
                    if text:
                        content_parts.append(text)
                    sibling = sibling.find_next_sibling()
                # Nếu content_parts rỗng, lấy parent content
                if not content_parts:
                    parent = h2.parent
                    if parent:
                        full_text = clean(parent.get_text())
                        h2_text = clean(h2.get_text())
                        if full_text.startswith(h2_text):
                            full_text = full_text[len(h2_text):].strip()
                        content_parts = [full_text] if full_text else []
                sections[heading] = " ".join(content_parts)

            # Company: tìm từ metadata hoặc title tag
            page_title = soup.select_one("title")
            if page_title:
                title_text = page_title.get_text()
                # Pattern: "... at {CompanyName} T11/2026"
                comp_match = re.search(r"at\s+(.+?)\s+T\d+/\d+", title_text)
                if comp_match:
                    rec["company_name"] = clean(comp_match.group(1))

            # Fallback company từ body text
            if not rec["company_name"]:
                body_text = soup.get_text()
                # Tìm pattern công ty
                for pattern in [r"(Công [Tt]y\s+.{5,80}?)(?:\n|Ho Chi|Hà Nội|Hồ Chí)",
                                r"(CÔNG TY\s+.{5,80}?)(?:\n|Ho Chi|Hà Nội|Hồ Chí)"]:
                    m = re.search(pattern, body_text)
                    if m:
                        rec["company_name"] = clean(m.group(1))
                        break

            # Salary: pattern matching từ body text
            body_text = soup.get_text()
            sal_patterns = [
                r"(Negotiable|Thương lượng)",
                r"(\d+[\s,]*-[\s,]*\d+\s*(?:triệu|tr|million|USD|\$|VND))",
                r"(Up to\s+\$[\d,]+)",
                r"(\$[\d,]+\s*-\s*\$[\d,]+)",
                r"(Lên đến\s+[\d,]+\s*(?:triệu|tr|VND))",
            ]
            for pat in sal_patterns:
                m = re.search(pat, body_text, re.IGNORECASE)
                if m:
                    rec["salary_raw"] = clean(m.group(1))
                    break

            # Location
            loc_section = sections.get("job locations", "")
            if loc_section:
                rec["location"] = clean(loc_section)[:200]
            if not rec["location"]:
                # Pattern matching
                loc_match = re.search(
                    r"(Ho Chi Minh|Hà Nội|Hồ Chí Minh|Da Nang|Đà Nẵng|"
                    r"Hai Phong|Hải Phòng|Binh Duong|Bình Dương|Can Tho|Cần Thơ)",
                    body_text, re.IGNORECASE
                )
                if loc_match:
                    rec["location"] = loc_match.group(1)

            # Experience: từ Job Information section
            info_section = sections.get("job information", "")
            if info_section:
                exp_match = re.search(
                    r"(\d+\s*-\s*\d+\s*(?:years?|năm)|Over\s*\d+\s*years?|"
                    r"\d+\+?\s*years?|Fresher|Junior|Senior|"
                    r"Less than\s*\d+\s*years?|Entry.level|"
                    r"Không yêu cầu kinh nghiệm|\d+ năm)",
                    info_section, re.IGNORECASE
                )
                rec["experience_required"] = exp_match.group(0) if exp_match else ""

            # Job description
            rec["job_description"] = sections.get("job description", "")

            # Requirements + Benefits
            req = sections.get("job requirements", "")
            ben = sections.get("what we can offer", "")
            parts = [p for p in [req, ben] if p]
            rec["requirements_and_benefits"] = " ║ ".join(parts)

            # Fallback: nếu sections rỗng, dùng main content text
            if not rec["job_description"]:
                main = soup.select_one("main, [role='main']")
                if main:
                    rec["job_description"] = clean(main.get_text())[:3000]

        except Exception as e:
            log.warning("[VNW] ✗ Lỗi trích xuất %s: %s", url, e)
            return None

        return rec if rec["job_title"] else None

    # ── Vòng lặp chính cho 1 từ khóa ─────────────────────────────────────
    async def scrape_keyword(self, keyword: str, target: int) -> int:
        added = 0
        url = self.search_url(keyword)
        log.info("[VNW] KW='%s' → %s", keyword, url)

        try:
            await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
            await human_delay(3.0, 5.0)
        except Exception as e:
            log.warning("[VNW] ✗ Lỗi tải search: %s", e)
            return 0

        for page_num in range(1, self.max_pages + 1):
            if self.store.count >= target:
                log.info("[VNW] 🎯 Đã đạt mục tiêu %d → dừng", target)
                return added

            log.info("[VNW] KW='%s' trang %d", keyword, page_num)
            links = await self._collect_links_from_current_page()
            log.info("[VNW]   → %d link mới", len(links))

            if not links:
                log.info("[VNW] Hết kết quả '%s' trang %d", keyword, page_num)
                break

            for link in links:
                if self.store.count >= target:
                    return added
                rec = await self._extract_detail(link, keyword)
                if rec and self.store.add(rec):
                    added += 1
                    log.info(
                        "[VNW] ✓ #%d │ %s │ %s",
                        self.store.count,
                        rec["job_title"][:55],
                        rec["company_name"][:35],
                    )

            # Quay lại trang search để chuyển trang
            try:
                await self.page.go_back(timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await human_delay(2.0, 4.0)
            except Exception:
                # Nếu go_back fail, navigate lại
                await self.page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await human_delay(3.0, 5.0)

            # Chuyển trang bằng click button
            if not await self._go_to_next_page():
                log.info("[VNW] Hết phân trang cho '%s'", keyword)
                break

            # Lưu tăng dần
            self.store.save_incremental()

        return added


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI & MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="🔍 Web Scraper — Tuyển dụng AI/ML/Data Science tại Việt Nam",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ sử dụng:
  python scraper.py --demo --max-pages 2       Demo trước hội đồng
  python scraper.py --full --target 1200       Thu thập đầy đủ
  python scraper.py --full --target 800 --keywords "AI Engineer,LLM,NLP"
        """,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--demo", action="store_true",
        help="Chế độ demo: headless=False, in log chi tiết, giới hạn theo --max-pages",
    )
    mode.add_argument(
        "--full", action="store_true",
        help="Chế độ thu thập đầy đủ: headless=True",
    )
    parser.add_argument(
        "--target", type=int, default=1000,
        help="Mục tiêu số lượng tin (mặc định: 1000)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=50,
        help="Số trang tối đa mỗi từ khóa (mặc định: 50)",
    )
    parser.add_argument(
        "--keywords", type=str, default=None,
        help='Từ khóa (phân cách bằng dấu phẩy). Mặc định: bộ AI/ML/DS',
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
        [k.strip() for k in args.keywords.split(",")]
        if args.keywords
        else DEFAULT_KEYWORDS
    )

    store = DataStore(clean_start=args.clean_start)

    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  Job Scraper — AI/ML/Data Science — Việt Nam               ║")
    log.info("╠══════════════════════════════════════════════════════════════╣")
    log.info("║  Chế độ     : %s", "DEMO (headless=False)" if args.demo else "FULL (headless=True)")
    log.info("║  Mục tiêu   : %d tin (Hiện có: %d bản ghi trong file)", target, store.count)
    log.info("║  Max pages  : %d/từ khóa", max_pages)
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
        # Chặn tải ảnh/font/media để tăng tốc (giữ CSS & JS)
        await context.route(
            re.compile(r"\.(png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf|eot|mp4|webm)$"),
            lambda route: route.abort(),
        )

        page = await context.new_page()

        # ── 1) Cào TopCV ─────────────────────────────────────────────────
        log.info("▶  PHASE 1: TopCV.vn")
        topcv = TopCVScraper(page, store, max_pages)
        for kw in keywords:
            if store.count >= target:
                break
            count = await topcv.scrape_keyword(kw, target)
            log.info("[TopCV] KW='%s' → %d tin mới │ Tổng: %d", kw, count, store.count)
            # Xoay User-Agent giữa các keyword
            await context.close()
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=pick_ua(),
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
            )
            await context.route(
                re.compile(r"\.(png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf|eot|mp4|webm)$"),
                lambda route: route.abort(),
            )
            page = await context.new_page()
            topcv.page = page

        # ── 2) Cào VietnamWorks ──────────────────────────────────────────
        if store.count < target:
            log.info("▶  PHASE 2: VietnamWorks.com")
            vnw = VietnamWorksScraper(page, store, max_pages)
            for kw in keywords:
                if store.count >= target:
                    break
                count = await vnw.scrape_keyword(kw, target)
                log.info("[VNW] KW='%s' → %d tin mới │ Tổng: %d", kw, count, store.count)
                # Xoay User-Agent
                await context.close()
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent=pick_ua(),
                    locale="vi-VN",
                    timezone_id="Asia/Ho_Chi_Minh",
                )
                await context.route(
                    re.compile(r"\.(png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf|eot|mp4|webm)$"),
                    lambda route: route.abort(),
                )
                page = await context.new_page()
                vnw.page = page
        else:
            log.info("▶  Đã đủ mục tiêu — bỏ qua VietnamWorks")

        await browser.close()

    # ── Lưu cuối cùng ────────────────────────────────────────────────────
    store.save_all()

    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  ✅  HOÀN TẤT                                              ║")
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

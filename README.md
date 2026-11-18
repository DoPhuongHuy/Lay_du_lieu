# 🔍 Job Scraper — AI / ML / Data Science — Việt Nam

Công cụ thu thập dữ liệu tuyển dụng tự động từ **TopCV.vn** và **VietnamWorks.com**, phục vụ nghiên cứu thị trường lao động phân khúc AI / Machine Learning / Data Science tại Việt Nam.

## 📦 Cài đặt

```bash
# 1. Cài thư viện Python
pip install -r requirements.txt

# 2. Cài trình duyệt Chromium cho Playwright
playwright install chromium
```

## 🚀 Cách sử dụng

### Chế độ Demo (trình diễn trước hội đồng)
```bash
# Mở giao diện trình duyệt, cào 2 trang đầu, in log trực tiếp
python scraper.py --demo --max-pages 2
```

### Chế độ Full (thu thập đầy đủ, Playwright chạy ngầm)
```bash
# Thu thập 1200 tin, trình duyệt chạy ngầm (headless):
python scraper.py --full --target 1200

# Tùy chỉnh từ khóa:
python scraper.py --full --target 800 --keywords "AI Engineer,LLM,NLP"

# Chạy ngầm hoàn toàn dưới nền Windows (không chiếm cửa sổ terminal):
Start-Process python -ArgumentList "scraper.py --full --target 1200" -WindowStyle Hidden

# Theo dõi tiến độ chạy ngầm bất kỳ lúc nào:
Get-Content scraper.log -Wait -Tail 20
```

### Tham số dòng lệnh

| Tham số | Mô tả | Mặc định |
|---------|--------|----------|
| `--demo` | Chế độ demo: headless=False, hiện trình duyệt | — |
| `--full` | Chế độ đầy đủ: headless=True | — |
| `--target N` | Mục tiêu số lượng tin tuyển dụng | 1000 |
| `--max-pages N` | Số trang tối đa mỗi từ khóa | 50 |
| `--keywords "k1,k2"` | Danh sách từ khóa (phân cách bởi dấu phẩy) | AI/ML/DS keywords |
| `--clean-start` | Xóa checkpoint, bắt đầu thu thập mới | False |

## 📊 Dữ liệu đầu ra

### File output
- `raw_jobs_data.csv` — CSV (UTF-8 BOM, mở Excel không lỗi font)
- `raw_jobs_data.json` — JSON (UTF-8)

### Schema (11 trường)

| # | Trường | Mô tả |
|---|--------|--------|
| 1 | `scraped_timestamp` | Thời gian thu thập (YYYY-MM-DD HH:MM:SS) |
| 2 | `source_platform` | Nền tảng nguồn (TopCV / VietnamWorks) |
| 3 | `search_keyword` | Từ khóa tìm kiếm |
| 4 | `job_url` | Link gốc tin tuyển dụng |
| 5 | `job_title` | Tiêu đề công việc |
| 6 | `company_name` | Tên công ty |
| 7 | `location` | Địa điểm làm việc |
| 8 | `salary_raw` | Mức lương gốc (chưa chuẩn hóa) |
| 9 | `experience_required` | Yêu cầu kinh nghiệm |
| 10 | `job_description` | Mô tả công việc chi tiết |
| 11 | `requirements_and_benefits` | Yêu cầu ứng viên & Quyền lợi |

## 🛡️ Cơ chế chống crash & lưu tiếp nối (Append mode)

- **Ghi tiếp nối (Append Mode)**: Tự động nạp toàn bộ dữ liệu đang có từ `raw_jobs_data.csv` khi khởi động. Dữ liệu mới cào sẽ được ghi nối tiếp vào file, **không bao giờ bị ghi đè hay mất dữ liệu cũ**.
- **Khử trùng lặp tự động (De-duplication)**: Tự động nhớ các `job_url` đã có trong file, lập tức bỏ qua các tin đã cào để tiết kiệm thời gian và tài nguyên mạng.
- **Delay ngẫu nhiên** 2-5 giây giữa mỗi request.
- **Xoay vòng User-Agent** giữa các từ khóa (pool 6 UA).
- **Try-except bọc từng tin** — tin lỗi bị bỏ qua, không crash.
- **Checkpoint tự động** mỗi 25 tin (file `.scraper_checkpoint.json`).
- **Lưu tăng dần** (incremental save) sau mỗi trang.
- **Chặn tải ảnh/font** — tăng tốc 2-3x.

## 🔧 Kiến trúc mã nguồn

```
scraper.py              ← File chính duy nhất
├── DataStore           ← Quản lý dữ liệu: dedup, checkpoint, CSV/JSON
├── TopCVScraper        ← Scraper cho TopCV.vn (selector xác minh thực tế)
├── VietnamWorksScraper ← Scraper cho VietnamWorks.com (selector xác minh thực tế)
├── parse_args()        ← CLI argument parser
└── main()              ← Điều phối tổng thể

requirements.txt        ← Thư viện cần cài
README.md              ← Tài liệu hướng dẫn (file này)
```

## ⚠️ Lưu ý đạo đức

Script chỉ dùng cho mục đích **nghiên cứu / học thuật**. Vui lòng:
- Kiểm tra `robots.txt` và Điều khoản Sử dụng (ToS) của từng trang
- Không chạy song song nhiều instance
- Giữ delay giữa các request ≥ 2 giây

## 📋 Từ khóa mặc định

`AI Engineer`, `Machine Learning`, `Data Analyst`, `Data Science`, `Python`, `SQL`, `PyTorch`, `LLM`

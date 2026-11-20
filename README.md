# 🔍 Multi-Platform Job Scraper — Data & IT Sector — Vietnam

Công cụ thu thập dữ liệu tuyển dụng tự động đa nền tảng tại Việt Nam, phục vụ nghiên cứu và phân tích thị trường lao động phân khúc Công nghệ thông tin (Data Science, AI, Backend, Frontend, DevOps, Tester...).

## 🌐 Các nền tảng hỗ trợ (2 nền tảng)
1. **VietnamWorks.com**
2. **TopCV.vn**

---

## 📦 Cài đặt

```bash
# 1. Cài thư viện Python cần thiết
pip install -r requirements.txt

# 2. Cài đặt trình duyệt Chromium cho Playwright
playwright install chromium
```

---

## 🚀 Hướng dẫn sử dụng

### 1. Chế độ thu thập đầy đủ (Headless - Chạy ngầm trong RAM)
```bash
# Cào đầy đủ tất cả nền tảng với mục tiêu 1200 tin:
python scraper.py --full --target 1200

# Chỉ cào trên nền tảng mong muốn (ví dụ: VietnamWorks hoặc TopCV):
python scraper.py --full --target 800 --platforms vietnamworks,topcv

# Tùy biến từ khóa tìm kiếm:
python scraper.py --full --target 500 --keywords "AI Engineer,Data Science,Python"
```

### 2. Chế độ Demo (Hiện trình duyệt kiểm chứng trực quan)
```bash
# Mở trình duyệt trực quan, tự động gõ từ khóa vào ô search & bấm nút tìm kiếm:
python scraper.py --demo --max-pages 1 --keywords "Data Science,Python" --platforms topcv,vietnamworks
```

### 3. Cào mới hoàn toàn từ đầu (Làm rỗng file dữ liệu)
```bash
python scraper.py --full --target 1200 --clean-start
```

### 3. Tham số dòng lệnh (CLI Parameters)

| Tham số | Ý nghĩa | Mặc định |
|---|---|---|
| `--full` | Chế độ chạy ngầm (Headless = True), không bật cửa sổ trình duyệt | — |
| `--demo` | Chế độ demo (Headless = False), hiển thị cửa sổ trình duyệt | — |
| `--target N` | Mục tiêu số lượng tin tuyển dụng cần đạt được | `1000` |
| `--max-pages N` | Số trang tối đa mỗi từ khóa trên từng trang | `30` |
| `--platforms list` | Danh sách nền tảng (`topcv,vietnamworks`) | Cào tất cả |
| `--keywords list` | Bộ từ khóa tìm kiếm tùy chỉnh, phân cách bởi dấu phẩy | Bộ IT/Data mặc định |
| `--clean-start` | Xóa dữ liệu cũ / checkpoint, cào lại từ đầu | Tiếp nối dữ liệu cũ |

---

## 🛡️ Điểm nổi bật & Cơ chế chống lỗi
* **Kiến trúc Tab độc lập (Isolated Detail Tab):** Trang danh sách tìm kiếm (`search_page`) được cố định riêng biệt; việc cào chi tiết JD được thực hiện trong tab tạm thời rồi đóng lại ngay. Khắc phục triệt để lỗi mất DOM và kẹt phân trang.
* **Ghi tiếp nối (Append Mode):** Tự động nạp dữ liệu hiện có từ `raw_jobs_data.csv`, không bao giờ ghi đè làm mất dữ liệu cũ.
* **Khử trùng lặp theo `job_url`:** Tự động nhận diện và bỏ qua các link đã cào để tiết kiệm thời gian.
* **Lưu tăng dần (Incremental Checkpoint):** Tự động lưu sau mỗi trang và mỗi 25 tin mới.
* **Ghi nhật ký kép (Dual Logging):** In trực tiếp ra Terminal và đồng thời ghi vào `scraper.log`.

# Glamira Product Collector

Công cụ thu thập dữ liệu sản phẩm Glamira từ MongoDB, crawl thông tin chi tiết và
xuất kết quả ra JSONL. Tiến trình được lưu trong SQLite nên có thể tiếp tục sau khi
chương trình bị dừng.

## Yêu cầu

- Python 3.11 trở lên
- Poetry
- Quyền truy cập MongoDB nguồn
- File IP2Location DB5 nếu dùng lệnh `locations`

Crawler sử dụng `curl-cffi`, không cần cài Chrome, Edge hoặc Playwright.

## Cài đặt

Chạy các lệnh sau tại thư mục gốc của dự án:

```powershell
poetry env use 3.11
poetry install
```

Kiểm tra CLI sau khi cài đặt:

```powershell
poetry run python main.py --help
```

Sau `poetry install`, có thể dùng entry point `glamira-crawl` thay cho
`python main.py`. Hai lệnh dưới đây tương đương:

```powershell
poetry run python main.py stats
poetry run glamira-crawl stats
```

Các ví dụ còn lại trong README sử dụng cách gọi `poetry run python main.py`.

## Cấu hình MongoDB

Cấu hình chung nằm tại `config/config.yml`. Không lưu tài khoản hoặc mật khẩu thật
trong file này. Tạo file `.env` tại thư mục gốc của dự án với nội dung:

```dotenv
MONGODB_USERNAME=your-user
MONGODB_PASSWORD=your-password
MONGODB_AUTH_SOURCE=admin
```

Hoặc cung cấp connection string đầy đủ:

```dotenv
MONGODB_URI=mongodb://user:password@mongo-host:27017/?authSource=admin
```

Nếu URI chứa ký tự đặc biệt, username và password phải được URL-encode. Biến môi
trường của process được ưu tiên hơn giá trị trong `.env` và `config/config.yml`.

Ví dụ đặt biến trực tiếp trong PowerShell:

```powershell
$env:MONGODB_USERNAME = "your-user"
$env:MONGODB_PASSWORD = "your-password"
$env:MONGODB_AUTH_SOURCE = "admin"
poetry run python main.py discover
```

## Các lệnh chạy chương trình

### Chạy toàn bộ pipeline

Lệnh sau lần lượt thực hiện `discover`, `crawl` và `export`:

```powershell
poetry run python main.py run
```

Để thử lại một lần các sản phẩm đã crawl thất bại:

```powershell
poetry run python main.py run --retry-failed
```

### Chạy từng bước

```powershell
# 1. Quét MongoDB và đưa product_id/URL vào hàng đợi SQLite
poetry run python main.py discover

# 2. Crawl các sản phẩm đang chờ
poetry run python main.py crawl

# 3. Xem số lượng item theo trạng thái trong hàng đợi
poetry run python main.py stats

# 4. Xuất kết quả ra JSONL
poetry run python main.py export
```

Các tuỳ chọn bổ sung:

```powershell
# Thử lại một lần các sản phẩm đã thất bại
poetry run python main.py crawl --retry-failed

# Xuất sản phẩm nhưng không thêm object _crawl
poetry run python main.py export --no-metadata
```

Checkpoint và kết quả trung gian được lưu tại `data/crawl-state.sqlite3`. Khi
chương trình bị dừng, chạy lại cùng lệnh để tiếp tục; không cần quét lại từ đầu.

### Xuất vị trí theo địa chỉ IP

Đặt database IP2Location tại `data/IP2LOCATION-LITE-DB5.BIN`, sau đó chạy:

```powershell
poetry run python main.py locations
```

Mặc định lệnh dùng số worker được khai báo trong chương trình. Có thể chỉ định số
worker khác, ví dụ:

```powershell
poetry run python main.py locations --workers 16
```

Kết quả được ghi lại từ đầu vào `data/locations.jsonl` sau mỗi lần chạy.

### Dùng file cấu hình khác

Tuỳ chọn `--config` phải đặt trước tên subcommand:

```powershell
poetry run python main.py --config path/to/config.yml run
poetry run python main.py --config path/to/config.yml crawl --retry-failed
```

## Danh sách lệnh nhanh

| Lệnh | Chức năng |
| --- | --- |
| `discover` | Quét MongoDB và tạo hàng đợi product ID/URL |
| `crawl` | Crawl các sản phẩm đang chờ |
| `crawl --retry-failed` | Crawl và thử lại một lần các item thất bại |
| `stats` | Xem trạng thái hàng đợi |
| `export` | Xuất sản phẩm và URL lỗi ra JSONL |
| `export --no-metadata` | Xuất sản phẩm không kèm `_crawl` |
| `run` | Chạy `discover`, `crawl`, `export` liên tiếp |
| `run --retry-failed` | Chạy toàn bộ pipeline và thử lại item thất bại |
| `locations [--workers N]` | Xuất location của các IP duy nhất |

Để xem trợ giúp của một lệnh cụ thể:

```powershell
poetry run python main.py crawl --help
poetry run python main.py locations --help
```

## File đầu ra

- `data/products.jsonl`: dữ liệu sản phẩm; mỗi dòng là một JSON object.
- `data/failed-urls.jsonl`: URL lỗi, số lần lỗi và URL fallback nếu có.
- `data/locations.jsonl`: thông tin vị trí của các IP duy nhất.
- `data/crawl-state.sqlite3`: checkpoint, hàng đợi và kết quả trung gian.

Nếu URL tracking không truy cập được, crawler tự thử URL chuẩn theo mẫu:

```text
https://www.glamira.co.uk/catalog/product/view/id/{product_id}
```

## Tuỳ chỉnh và vận hành

Có thể chỉnh MongoDB, đường dẫn file, field đầu ra, User-Agent, delay, retry,
concurrency, batch size và mức log trong `config/config.yml`. Đặt
`product_fields: null` để lưu toàn bộ object sản phẩm.

Các mức log thường dùng:

- `DEBUG`: chi tiết request, response và retry.
- `INFO`: tiến độ và kết quả từng sản phẩm.
- `WARNING`: cảnh báo, URL lỗi và retry.
- `ERROR`: chỉ các lỗi nghiêm trọng.

Với collection rất lớn, nên dùng mức `WARNING` để giảm lượng log. Pipeline stream
dữ liệu theo batch và lưu trạng thái trên đĩa, không nạp toàn bộ product ID vào RAM.

## Chạy kiểm thử

```powershell
poetry run python -m unittest discover -s tests -v
```

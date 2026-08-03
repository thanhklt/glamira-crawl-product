# Glamira product collector

Pipeline có checkpoint để quét collection MongoDB lớn, khử trùng lặp theo `product_id`,
trích `react_data_url` từ trang sản phẩm và xuất thông tin sản phẩm ra JSONL.

Nếu URL lấy từ tracking không truy cập được, crawler tự động thử URL chuẩn theo
ID: `https://www.glamira.co.uk/catalog/product/view/id/{product_id}`. Tất cả URL
lỗi được xuất ra `data/failed-urls.jsonl`, kể cả khi URL fallback sau đó thành
công.

## Cài đặt

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

Nếu PowerShell không cho activate script, có thể gọi trực tiếp
`.\.venv\Scripts\python.exe main.py <command>`.

Playwright dùng trực tiếp Google Chrome và Microsoft Edge đã cài trên Windows,
không cần tải thêm Chromium. Kiểm tra browser và User-Agent bằng:

```powershell
python main.py browser-check
```

## Xác thực MongoDB

Không lưu credential thật trong `config.yml`. Ứng dụng tự đọc file `.env` ở thư
mục project; có thể sao chép `.env.example` thành `.env`. File `.env` đã nằm
trong `.gitignore`. Username/password được truyền riêng cho PyMongo nên không
cần URL-encode.

```dotenv
MONGODB_USERNAME=your-user
MONGODB_PASSWORD=your-password
```

Biến môi trường của process có độ ưu tiên cao hơn file `.env`. Có thể đặt trực
tiếp bằng PowerShell:

```powershell
$env:MONGODB_USERNAME = "your-user"
$env:MONGODB_PASSWORD = "your-password"
$env:MONGODB_AUTH_SOURCE = "admin"
python main.py discover
```

Hoặc dùng connection string đầy đủ. Với cách này, username/password trong URI phải
được URL-encode nếu có ký tự đặc biệt:

```powershell
$env:MONGODB_URI = "mongodb://user:password@mongo-host:27017/?authSource=admin"
python main.py discover
```

## Chạy pipeline

Nên chạy từng bước để dễ theo dõi:

```powershell
# 1. Stream document từ MongoDB và ghi product_id/URL vào queue SQLite
python main.py discover

# 2. Crawl các product đang pending
python main.py crawl

# 3. Xem tiến độ
python main.py stats

# 4. Xuất data/products.jsonl và data/failed-urls.jsonl
python main.py export
```

Có thể chạy cả ba bước bằng `python main.py run`. Khi process bị dừng, chạy lại
cùng lệnh; checkpoint MongoDB và trạng thái crawl nằm trong
`data/crawl-state.sqlite3`. Các product thất bại không bị thử lại vô hạn; dùng:

```powershell
python main.py crawl --retry-failed
```

Mỗi dòng trong output là một JSON object. Khóa `_crawl` chứa URL nguồn và thời
gian crawl; dùng `python main.py export --no-metadata` nếu chỉ cần các field sản phẩm.

Trong lúc crawl, JSON sản phẩm được lưu bền vững ở bảng `results` trong
`data/crawl-state.sqlite3`. Lệnh `export` chuyển chúng thành
`data/products.jsonl`; file `data/failed-urls.jsonl` chứa ID, URL lỗi, lỗi gần
nhất, số lần lỗi và URL fallback đã phục hồi sản phẩm (nếu có).

### Log trên terminal

Crawler ghi log có timestamp trực tiếp ra terminal. Mức log được cấu hình tại:

```yaml
logging:
  level: INFO
```

- `INFO`: từng product ID, tracking/fallback URL, kết quả và tiến độ batch.
- `DEBUG`: thêm từng HTTP request, response và retry.
- `WARNING`: chỉ URL lỗi, retry và cảnh báo.
- `ERROR`: chỉ các sản phẩm thất bại hoàn toàn.

Với tập dữ liệu rất lớn, nên dùng `WARNING` để tránh terminal trở thành nút thắt
hiệu năng.

## Lưu ý cho collection 41 triệu document

- Pipeline dùng cursor theo batch và SQLite trên đĩa, không nạp toàn bộ ID vào RAM.
- Dữ liệu được checkpoint theo `_id`; mỗi lần chạy lại chỉ đọc các document mới hơn.
- Nên kiểm tra `explain()` trước khi chạy production. Index phù hợp cho truy vấn
  discovery thường là `{ collection: 1, _id: 1 }`; việc tạo index phải được DBA
  xác nhận vì có thể tốn dung lượng và I/O lớn.
- `crawler.concurrency` hiện là 3 và `per_host_limit` là 1.
- Bộ điều tiết chung giãn thời điểm bắt đầu mọi request từ 2 đến 3 giây
  (`request_delay_seconds` + jitter).
- User-Agent được lấy round-robin từ `crawler.user_agents`; cấu hình hiện dùng
  Chrome/Edge cài thật trên máy. Mỗi User-Agent có HTTP session và cookie jar
  riêng, và cùng một sản phẩm giữ nguyên profile cho cả HTML lẫn JSON.
- HTTP 401/403/404 không retry cùng URL. Chỉ 429, một số 5xx, timeout và lỗi mạng
  tạm thời được retry.
- `playwright.primary: true` khiến mọi candidate URL được mở bằng Chrome/Edge
  ngay từ lần đầu; `aiohttp` không chạy trước. Browser chạy JavaScript, giữ cookie
  riêng theo profile và gọi JSON trong cùng context. Trang Access Denied/CAPTCHA
  được ghi lỗi, không bị tự động vượt qua.
- Mỗi `product_id` giữ tối đa ba candidate URL. Chỉ một JSON thành công được
  lưu nhờ primary key trong SQLite.

Danh sách field output, User-Agent, delay, concurrency, batch size và các đường
dẫn đều có thể chỉnh trong `config.yml`. Đặt `product_fields: null` để lưu toàn
bộ object sản phẩm.

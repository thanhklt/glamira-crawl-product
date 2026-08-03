# Báo cáo triển khai Glamira Product Collector

## 1. Mục tiêu

Xây dựng pipeline đọc dữ liệu tracking từ MongoDB có thể chứa khoảng 41 triệu
document, lấy các cặp `product_id` và URL sản phẩm, sau đó:

1. Mở trang sản phẩm.
2. Tìm biến JavaScript `react_data_url` trong HTML.
3. Gọi URL JSON được tìm thấy.
4. Lấy các trường thông tin sản phẩm được yêu cầu.
5. Chỉ lưu một kết quả crawl thành công cho mỗi `product_id` khác nhau.
6. Có thể tiếp tục chạy sau khi process bị dừng mà không phải làm lại từ đầu.

## 2. Các event được đọc từ MongoDB

Pipeline lọc trường `collection` theo các giá trị sau:

- `view_product_detail`
- `select_product_option`
- `select_product_option_quality`
- `add_to_cart_action`
- `product_detail_recommendation_visible`
- `product_detail_recommendation_noticed`
- `product_view_all_recommend_clicked`

Với sáu event đầu tiên:

- Lấy `product_id`.
- Nếu `product_id` không có hoặc rỗng thì dùng `viewing_product_id`.
- Lấy URL từ `current_url`.

Với `product_view_all_recommend_clicked`:

- Lấy ID từ `viewing_product_id`.
- Lấy URL từ `referrer_url`.

Chỉ document thiếu ID mới bị bỏ qua. Nếu có ID nhưng URL thiếu hoặc không thuộc
giao thức HTTP/HTTPS, sản phẩm vẫn được đưa vào queue để crawler thử URL fallback
tạo từ ID.

## 3. Kiến trúc đã triển khai

Pipeline được chia thành ba giai đoạn độc lập:

```text
MongoDB
   |
   | discover: stream theo batch, lọc event và chuẩn hóa ID/URL
   v
SQLite crawl-state.sqlite3
   |
   | crawl: lấy từng product_id pending, thử các URL ứng viên
   v
SQLite results
   |
   | export: xuất nguyên tử qua file tạm
   v
data/products.jsonl
```

SQLite được dùng làm hàng đợi bền vững và checkpoint trên đĩa. Việc này tránh
giữ toàn bộ tập ID trong RAM và phù hợp hơn với nguồn dữ liệu lớn.

Ngoài file sản phẩm, pipeline còn xuất lịch sử URL lỗi:

```text
data/failed-urls.jsonl
```

### Vì sao stream vào SQLite?

MongoDB được stream theo batch, còn SQLite nhận dữ liệu đã chuẩn hóa theo từng
transaction nhỏ. Lý do chọn cách này:

1. Collection có khoảng 41 triệu document. Đưa toàn bộ document hoặc tập
   `product_id` distinct vào `list`, `set` hay DataFrame có thể sử dụng hết RAM.
2. Primary key và unique constraint của SQLite thực hiện dedupe trên đĩa. Không
   cần giữ hàng triệu ID trong bộ nhớ Python.
3. SQLite lưu checkpoint `_id`, trạng thái `pending`, `in_progress`, `done`,
   `failed` và số lần thử. Process có thể dừng rồi chạy tiếp.
4. Kết quả JSON và trạng thái `done` được cập nhật trong transaction. Điều này
   tránh trường hợp đã ghi output nhưng chưa đánh dấu job, hoặc ngược lại.
5. SQLite cho phép tách discovery, crawl và export thành các bước độc lập. Có
   thể kiểm tra số lượng, retry lỗi hoặc export lại mà không quét lại MongoDB và
   không crawl lại website.
6. JSONL vẫn là output cuối để transformation. SQLite không thay thế output mà
   là state store trung gian giúp pipeline chạy an toàn.

Đánh đổi là SQLite tạo thêm I/O và dung lượng đĩa. Đây là lựa chọn ưu tiên tính
resumable và tính đúng đắn hơn tốc độ tối đa. Nếu số product distinct quá lớn
hoặc cần nhiều crawler trên nhiều máy, queue nên được chuyển sang PostgreSQL,
Redis Streams, Kafka hoặc một collection MongoDB riêng.

## 4. Giai đoạn discovery

File thực hiện: `glamira_crawl/discovery.py`.

Các công việc chính:

- Kết nối MongoDB bằng PyMongo.
- Ưu tiên đọc từ secondary bằng `SecondaryPreferred` để giảm tải primary nếu
  MongoDB là replica set. Driver vẫn có thể đọc primary khi không có secondary.
- Chỉ project các trường cần thiết: `_id`, `collection`, `product_id`,
  `viewing_product_id`, `current_url` và `referrer_url`.
- Sắp xếp document theo `_id` tăng dần.
- Đọc MongoDB bằng cursor và `batch_size`, không gọi `list()` trên toàn bộ dữ
  liệu.
- Ghi checkpoint `_id` gần nhất vào SQLite sau mỗi nhóm document.
- Khi chạy lại, truy vấn bổ sung điều kiện `_id > checkpoint`.
- Chuẩn hóa ID về chuỗi để `85796`, `"85796"` có cùng khóa dedupe.

Checkpoint hiện giả định `_id` là MongoDB `ObjectId`, phù hợp với document mẫu.
Nếu collection sử dụng kiểu `_id` khác thì cần sửa phần phục hồi checkpoint.

### Khử trùng lặp và URL dự phòng

Mỗi `product_id` là primary key trong bảng `products`. Vì vậy một sản phẩm chỉ
có một job crawl, kể cả khi xuất hiện trong hàng nghìn tracking document.

Bảng `candidate_urls` có khóa duy nhất `(product_id, url)`. Mặc định hệ thống giữ
tối đa ba URL khác nhau cho mỗi sản phẩm. Nếu URL đầu tiên đã chết hoặc trả sai
sản phẩm, crawler sẽ thử URL tiếp theo.

Nếu sản phẩm có ID nhưng không có candidate URL hợp lệ, job vẫn được tạo với
danh sách URL rỗng. Crawler sau đó dùng URL fallback theo ID.

Giá trị này có thể sửa bằng:

```yaml
discovery:
  max_urls_per_product: 3
```

## 5. Kết nối và xác thực MongoDB

File cấu hình YAML không chứa username/password thật. Ứng dụng tự đọc `.env`
trong thư mục project bằng `python-dotenv`; file này đã được thêm vào
`.gitignore`. Biến môi trường của process có độ ưu tiên cao hơn `.env`. Pipeline
hỗ trợ hai cách xác thực.

Workspace VS Code có `.vscode/settings.json` với
`python.terminal.useEnvFile: true`, do đó terminal Python mới cũng tự nhận các
biến trong `.env`. Cần đóng terminal cũ và mở terminal mới sau khi thay đổi.

### Cách khuyến nghị

Truyền username và password riêng cho PyMongo qua biến môi trường:

```powershell
$env:MONGODB_USERNAME = "your-user"
$securePassword = Read-Host "MongoDB password" -AsSecureString
$env:MONGODB_PASSWORD = [System.Net.NetworkCredential]::new("", $securePassword).Password
$env:MONGODB_AUTH_SOURCE = "admin"
```

Vì username/password được truyền riêng cho driver, password có ký tự đặc biệt
không cần URL-encode.

### Dùng connection string đầy đủ

```powershell
$env:MONGODB_URI = "mongodb://user:encoded-password@host:27017/?authSource=admin"
```

Nếu credential được đặt trong URI, các ký tự đặc biệt phải được URL-encode.

URI mặc định hiện là `mongodb://localhost:27017`. Cần sửa `mongodb.uri` trong
`config.yml` hoặc đặt `MONGODB_URI` nếu MongoDB chạy trên host khác.

`auth_source` mặc định là `admin`. Nếu MongoDB user được tạo trong database
`countly` thì phải đổi thành `countly`.

## 6. Giai đoạn crawl

File thực hiện: `glamira_crawl/crawler.py`.

Các công việc chính:

- Dùng `aiohttp` để gửi request bất đồng bộ.
- Giới hạn tổng số request đồng thời bằng `crawler.concurrency`.
- Giới hạn connection đồng thời trên từng domain bằng `per_host_limit`.
- Dùng một bộ điều tiết chung để giãn thời điểm bắt đầu mọi request theo
  `request_delay_seconds` cộng jitter ngẫu nhiên.
- Xoay vòng User-Agent theo sản phẩm; cùng một sản phẩm giữ nguyên User-Agent
  khi gọi trang HTML và endpoint JSON.
- Dùng chung một `ClientSession` để tái sử dụng connection, DNS cache và cookie.
- Theo redirect của trang sản phẩm.
- Retry các lỗi mạng, timeout, HTTP 429 và HTTP 5xx với exponential backoff.
- Giới hạn HTML ở 10 MB và JSON ở 20 MB để tránh response bất thường sử dụng
  quá nhiều RAM.
- Gửi `Referer` của trang sản phẩm khi gọi `react_data_url`.
- Thử lần lượt các candidate URL của cùng một sản phẩm.
- Sau khi các candidate URL thất bại, thử URL fallback:
  `https://www.glamira.co.uk/catalog/product/view/id/{product_id}`.
- Nếu không có candidate URL từ MongoDB, thử URL fallback ngay lập tức.
- Ghi lại từng URL lỗi kể cả khi fallback sau đó thành công.

Crawler hiện chạy ba job, một connection trên mỗi host, và giãn thời điểm bắt
đầu request từ 2 đến 3 giây:

```yaml
crawler:
  concurrency: 3
  per_host_limit: 1
  request_delay_seconds: 2.0
  request_jitter_seconds: 1.0
  request_timeout_seconds: 30
  retries: 3
  retry_backoff_seconds: 1.0
  retry_http_statuses: [429, 500, 502, 503, 504]
  user_agents:
    - Mozilla/5.0 (...) Chrome/150.0.0.0 Safari/537.36
    - Mozilla/5.0 (...) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0
  fallback_product_url_template: https://www.glamira.co.uk/catalog/product/view/id/{product_id}
```

`RequestPacer` được dùng chung cho tất cả worker. Vì vậy ba worker không tạo ba
request cùng lúc: thời điểm bắt đầu request toàn cục vẫn cách nhau ít nhất hai
giây cộng tối đa một giây jitter. Concurrency vẫn có ích khi một request đang chờ
response hoặc xử lý nội dung.

Nếu website tiếp tục trả nhiều 403/429, nên tăng delay trước khi cân nhắc tăng
concurrency. Nếu mạng chậm, có thể tăng timeout nhưng không nên tăng concurrency
cùng lúc khi chưa đo tải.

### User-Agent rotation

Danh sách `crawler.user_agents` có thể thêm hoặc bớt trong `config.yml`. Việc
phân phối sử dụng round-robin theo product:

```text
product 1 -> User-Agent 1
product 2 -> User-Agent 2
product 3 -> User-Agent 1
product 4 -> User-Agent 2
```

Hai profile hiện được lấy từ trình duyệt cài thật trên máy triển khai:

- Google Chrome `150.0.7871.187`, có chuỗi UA reduction `Chrome/150.0.0.0`.
- Microsoft Edge `151.0.4129.59`, có chuỗi UA reduction
  `Chrome/151.0.0.0 ... Edg/151.0.0.0`.

Config loader kiểm tra mỗi chuỗi phải bắt đầu bằng `Mozilla/5.0`, có tên browser
được hỗ trợ và version cụ thể. User-Agent không thay đổi giữa request HTML và
request `react_data_url` của cùng product.

Mỗi User-Agent sử dụng một `aiohttp.ClientSession`, connection pool và cookie jar
riêng. Điều này tránh tình trạng đổi User-Agent nhưng tái sử dụng cookie đã được
server cấp cho browser profile khác.

Giới hạn kỹ thuật: chuỗi User-Agent và header có thể giống trình duyệt, nhưng
`aiohttp` vẫn không có TLS/HTTP2 fingerprint, JavaScript runtime và browser
storage giống Chrome/Edge thật. Vì vậy User-Agent rotation không bảo đảm tránh
403. Nếu website yêu cầu browser challenge và việc tự động hóa được cho phép,
cần dùng browser runtime như Playwright thay cho HTTP client thuần.

### Playwright primary

Playwright hiện là đường crawl chính ngay từ URL đầu tiên. Cấu hình mặc định:

```yaml
playwright:
  enabled: true
  primary: true
  concurrency: 1
  navigation_timeout_seconds: 45
  wait_after_load_seconds: 1.5
  fallback_http_statuses: [403]
  fallback_when_react_data_url_missing: true
  browser_channels: [chrome, msedge]
```

Quy tắc hoạt động khi `primary: true`:

- Mỗi candidate URL được mở trực tiếp bằng Playwright; `aiohttp` không được gọi
  trước và cấu hình `crawler.retries` không áp dụng cho browser navigation.
- Browser channel và User-Agent dùng cùng profile round-robin của product.
- Chrome và Edge có browser context, connection state và cookie jar riêng.
- Browser chạy JavaScript rồi đọc `window.react_data_url`; nếu biến không global,
  parser tiếp tục tìm trong DOM sau render.
- Endpoint JSON được gọi qua `BrowserContext.request`, dùng cookie của browser
  context và `Referer` của trang sản phẩm.
- Playwright chỉ chạy một page đồng thời để giới hạn mức sử dụng RAM/CPU và giảm
  áp lực request lên website.
- Nếu phát hiện `Access Denied`, CAPTCHA, “verify you are human” hoặc challenge,
  code ghi lỗi blocked và không cố tương tác/vượt challenge.
- Nếu một URL thất bại, crawler chuyển sang candidate URL kế tiếp; URL fallback
  `/catalog/product/view/id/{product_id}` vẫn nằm cuối danh sách.

Có thể quay lại kiến trúc HTTP trước, browser fallback bằng cách đặt
`playwright.primary: false`. Khi đó Playwright chỉ được gọi cho các status trong
`fallback_http_statuses` hoặc khi thiếu `react_data_url`.

Máy hiện tại đã kiểm tra khởi động thành công cả channel `chrome` và `msedge`;
`navigator.userAgent` của cả hai khớp cấu hình. Có thể kiểm tra lại sau khi nâng
cấp browser bằng:

```powershell
python main.py browser-check
```

### Flow retry hiện tại

```mermaid
flowchart TD
    A[Claim product pending từ SQLite] --> B[Chọn User-Agent theo round-robin]
    B --> C[Tạo danh sách URL tracking và fallback ở cuối]
    C --> D[Chọn URL tiếp theo]
    D --> E[Chờ Playwright semaphore]
    E --> F[RequestPacer chờ delay cộng jitter]
    F --> G[Mở URL bằng Chrome hoặc Edge]
    G --> H{HTTP 2xx và không blocked?}
    H -->|Không| J[Ghi lỗi URL]
    H -->|Có| K[Chờ DOM và JavaScript]
    K --> L[Đọc window.react_data_url hoặc parse DOM]
    L --> M{Có react_data_url?}
    M -->|Không| J
    M -->|Có| N[RequestPacer chờ delay cộng jitter]
    N --> O[Gọi JSON bằng BrowserContext.request]
    O --> P{JSON hợp lệ và product_id khớp?}
    P -->|Có| R[Lưu result và đánh dấu done]
    P -->|Không| J
    J --> Q{Còn URL khác?}
    Q -->|Có| D
    Q -->|Không| S[Lưu url_failures và đánh dấu failed]
```

Quy tắc xử lý URL khi Playwright là primary:

| Trường hợp | Xử lý |
| --- | --- |
| HTTP khác 2xx | Ghi lỗi và chuyển URL tiếp theo |
| Access Denied/CAPTCHA/challenge | Ghi blocked và chuyển URL tiếp theo |
| Timeout/lỗi browser | Ghi lỗi và chuyển URL tiếp theo |
| Thiếu `react_data_url` sau render | Ghi lỗi và chuyển URL tiếp theo |
| JSON lỗi hoặc sai `product_id` | Ghi lỗi và chuyển URL tiếp theo |
| Candidate URL hết | Thử URL fallback; nếu vẫn lỗi thì đánh dấu failed |

Request top-level và request JSON đều đi qua `RequestPacer`. Các subresource do
trình duyệt tải trong lúc render được Chrome/Edge tự quản lý và không đi qua
`RequestPacer`.

### Log crawl trên terminal

CLI cấu hình Python logging với định dạng:

```text
2026-08-03 10:30:00 | INFO    | [85796] Thử URL 1/2 | type=tracking | url=...
```

Các sự kiện được ghi gồm:

- Cấu hình crawler khi bắt đầu: concurrency, retries và timeout.
- Product ID bắt đầu xử lý và tổng số URL sẽ thử.
- Thứ tự URL và loại `tracking`/`fallback`.
- HTTP retry, thời gian chờ và nguyên nhân.
- URL thất bại và thông báo lỗi.
- URL nguồn cùng `react_data_url` khi thành công.
- Tổng thành công/thất bại sau mỗi batch và khi crawler kết thúc.

Mức log chỉnh trong `config.yml`:

```yaml
logging:
  level: INFO
```

`DEBUG` hiển thị chi tiết HTTP; `INFO` phù hợp chạy sample; `WARNING` hoặc
`ERROR` phù hợp hơn khi chạy dữ liệu lớn để giảm I/O terminal.

## 7. Trích `react_data_url`

File thực hiện: `glamira_crawl/parsing.py`.

Parser tìm phép gán JavaScript có dạng:

```javascript
var react_data_url = 'https\u003A\u002F\u002Fexample.com\u002Fdata';
```

Các xử lý đã có:

- Hỗ trợ chuỗi dùng dấu nháy đơn hoặc nháy kép.
- Decode escape Unicode như `\u003A` và `\u002F`.
- Decode các JavaScript escape thông dụng.
- Decode HTML entity.
- Resolve URL tương đối dựa trên URL cuối cùng sau redirect.
- Chỉ chấp nhận URL HTTP/HTTPS hợp lệ.

Sau khi đọc JSON, code tìm object sản phẩm kể cả khi API bọc nó trong các object
như `data`, `result` hoặc `product`. Object ưu tiên phải có `product_id` và có
thêm `sku` hoặc `name`.

Kết quả JSON chỉ được chấp nhận khi `product_id` từ JSON khớp với ID lấy từ
MongoDB. Nếu không khớp, crawler thử URL dự phòng.

## 8. Các trường sản phẩm được lưu

Danh sách hiện được cấu hình trong `config.yml`:

- `product_id`
- `name`
- `sku`
- `attribute_set_id`
- `attribute_set`
- `type_id`
- `price`
- `min_price`
- `max_price`
- `min_price_format`
- `max_price_format`
- `gold_weight`
- `none_metal_weight`
- `fixed_silver_weight`
- `material_design`
- `qty`
- `collection`
- `collection_id`
- `product_type`
- `product_type_value`
- `category`
- `category_name`
- `store_code`
- `platinum_palladium_info_in_alloy`
- `bracelet_without_chain`
- `show_popup_quantity_eternity`
- `visible_contents`
- `gender`
- `configure_mode`
- `included_chain_weight`

Trường không xuất hiện trong JSON nguồn sẽ có giá trị `null`. Để lưu toàn bộ
object sản phẩm thay vì danh sách trên, sửa thành:

```yaml
product_fields: null
```

## 9. Trạng thái và tính resumable

File thực hiện: `glamira_crawl/state.py`.

SQLite có năm bảng:

| Bảng | Chức năng |
| --- | --- |
| `products` | Một dòng cho mỗi product ID và trạng thái crawl |
| `candidate_urls` | Các URL dự phòng của từng sản phẩm |
| `results` | JSON crawl thành công, khóa duy nhất theo product ID |
| `url_failures` | Lỗi gần nhất và số lần lỗi của từng product ID/URL |
| `metadata` | Checkpoint MongoDB và metadata khác |

Các trạng thái job:

- `pending`: đang chờ crawl.
- `in_progress`: đã được lấy ra để crawl.
- `done`: đã lấy và lưu JSON thành công.
- `failed`: đã thử hết candidate URL nhưng không thành công.

Nếu process bị tắt giữa chừng, các job `in_progress` được đưa về `pending` ở lần
chạy crawler kế tiếp. Kết quả và trạng thái được lưu trong cùng transaction, hạn
chế trạng thái dở dang.

Job `failed` không tự động lặp vô hạn. Lệnh `crawl --retry-failed` chỉ đưa các job
failed hiện tại về pending một lần trước khi bắt đầu lượt crawl mới.

Mỗi lỗi URL được upsert theo khóa `(product_id, url)`. Nếu fallback thành công,
trường `recovered_via` cho biết URL nào cuối cùng đã lấy được sản phẩm.

## 10. Xuất dữ liệu

Trong khi crawler chạy, dữ liệu sản phẩm được lưu trước tiên tại:

```text
data/crawl-state.sqlite3
```

Cụ thể, JSON nằm trong cột `results.payload`. Đây là nguồn dữ liệu bền vững để
resume và export lại. Sau khi chạy `export`, output phục vụ transformation nằm
tại:

```text
data/products.jsonl
```

Mỗi dòng là một JSON object độc lập, thuận tiện cho việc xử lý streaming ở bước
transform tiếp theo.

Mặc định mỗi object có thêm `_crawl`:

```json
{
  "_crawl": {
    "requested_product_id": "85796",
    "source_url": "https://example.com/product.html",
    "react_data_url": "https://example.com/react-data.json",
    "crawled_at": "2026-08-03T00:00:00+00:00"
  }
}
```

Có thể bỏ metadata bằng:

```powershell
python main.py export --no-metadata
```

Export ghi vào file `.tmp` trước rồi mới replace file đích. Nếu quá trình export
bị lỗi, file output hoàn chỉnh trước đó không bị ghi đè một phần.

URL lỗi được xuất riêng tại:

```text
data/failed-urls.jsonl
```

Mỗi dòng chứa:

- `product_id`
- `failed_url`
- `last_error`
- `occurrences`
- `last_failed_at`
- `recovered_via`: URL đã phục hồi được sản phẩm, hoặc `null` nếu tất cả URL đều
  thất bại.

File lỗi được tạo lại sau mỗi lệnh `crawl`, kể cả khi crawler bị ngắt bởi lỗi,
và cũng được tạo bởi lệnh `export`. Cách ghi qua file `.tmp` tránh file JSONL dở
dang.

## 11. Các lệnh sử dụng

### Chuẩn bị môi trường

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

### Chạy từng bước

```powershell
python main.py discover
python main.py crawl
python main.py stats
python main.py export
```

### Chạy toàn bộ

```powershell
python main.py run
```

### Thử lại lỗi

```powershell
python main.py crawl --retry-failed
```

Nếu PowerShell không cho activate virtual environment:

```powershell
.\.venv\Scripts\python.exe main.py stats
```

## 12. Các file đã tạo hoặc chỉnh sửa

| File | Nội dung |
| --- | --- |
| `main.py` | Entry point đơn giản cho CLI |
| `config.yml` | Cấu hình MongoDB, discovery, crawler, storage và field output |
| `pyproject.toml` | Dependency và console command `glamira-crawl` |
| `.env.example` | Mẫu các biến môi trường MongoDB, không chứa credential thật |
| `.vscode/settings.json` | Cho phép VS Code inject `.env` vào terminal Python mới |
| `glamira_crawl/config.py` | Đọc, kiểm tra và chuẩn hóa cấu hình |
| `glamira_crawl/discovery.py` | Đọc MongoDB, lọc event, lấy ID/URL và checkpoint |
| `glamira_crawl/parsing.py` | Trích URL JavaScript và tìm object sản phẩm |
| `glamira_crawl/crawler.py` | HTTP async, retry, URL và Playwright fallback |
| `glamira_crawl/browser.py` | Chrome/Edge contexts, JS render và JSON browser session |
| `glamira_crawl/state.py` | SQLite queue, dedupe, trạng thái, lỗi URL và export |
| `glamira_crawl/cli.py` | Các command và cấu hình định dạng logging terminal |
| `tests/test_discovery.py` | Test quy tắc lấy ID và URL theo event |
| `tests/test_parsing.py` | Test URL escape và JSON sản phẩm |
| `tests/test_state.py` | Test dedupe, URL limit và export |
| `tests/test_crawler.py` | Test retry bằng URL fallback theo product ID |
| `README.md` | Hướng dẫn cài đặt và vận hành |
| `.gitignore` | Bỏ qua credential file, virtualenv, SQLite và output |

## 13. Kiểm thử đã thực hiện

Đã chạy:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\glamira-crawl.exe --help
```

Kết quả:

- 17/17 unit test thành công, gồm kiểm tra Playwright primary không gọi HTTP
  client, 403 chuyển sang Playwright khi ở fallback mode, và 500 có HTTP retry.
- Không có dependency bị hỏng hoặc xung đột.
- CLI và phần hiển thị tiếng Việt chạy được trên PowerShell Windows.

Chưa chạy integration test với MongoDB và website thật vì chưa xác nhận endpoint
MongoDB, port, TLS, replica set và `authSource`. Credential được cung cấp không
được ghi vào repository.

## 14. Các điểm cần xác nhận trước production

1. Xác nhận host, port và `authSource` của MongoDB.
2. Xác nhận MongoDB có yêu cầu TLS, replica set hoặc SSH tunnel hay không.
3. Chạy `explain()` cho truy vấn discovery. Index đề xuất ban đầu là
   `{ collection: 1, _id: 1 }`, nhưng phải để DBA đánh giá dung lượng và tải I/O
   trước khi tạo trên collection 41 triệu document.
4. Chạy thử trên một tập dữ liệu nhỏ để xác nhận cấu trúc JSON thực tế của
   `react_data_url`.
5. Xác nhận website có yêu cầu cookie, proxy, header hoặc cơ chế chống bot bổ
   sung hay không.
6. Đo tỷ lệ HTTP 429/403 và điều chỉnh concurrency.
7. Xác nhận có cần giữ toàn bộ JSON hay chỉ các field hiện tại.
8. Xác nhận có chấp nhận URL từ mọi domain HTTP/HTTPS hay cần allowlist domain
   Glamira để tăng an toàn.
9. Ước lượng số product ID distinct và dung lượng SQLite/output trước khi chạy
   toàn bộ.

## 15. Hạn chế và hướng chỉnh sửa tiếp theo

- Discovery hiện hoàn thành trước rồi crawler mới bắt đầu khi dùng lệnh `run`.
  Nếu muốn giảm tổng thời gian, có thể chạy discovery và crawler song song bằng
  hai process vì SQLite đã bật WAL; tuy nhiên nên bổ sung kiểm thử cạnh tranh ghi
  trước khi áp dụng production.
- Checkpoint theo ObjectId phù hợp schema mẫu nhưng chưa hỗ trợ mọi kiểu `_id`.
- Crawler chạy trong một process. Nếu số product distinct rất lớn, có thể thêm
  cơ chế lease/worker ID để chạy nhiều máy.
- Delay hiện áp dụng toàn cục cho mọi domain. Đây là cấu hình thận trọng; nếu cần
  tối ưu throughput có thể nâng cấp thành rate limiter riêng cho từng domain.
- Chưa có Prometheus metric hoặc dashboard; trạng thái hiện xem qua lệnh `stats`
  và bảng SQLite.
- Lịch sử lỗi được gộp theo `(product_id, url)`: hệ thống giữ lỗi gần nhất và bộ
  đếm `occurrences`, không giữ nội dung đầy đủ của từng lần lỗi riêng lẻ.
- Việc kiểm tra ID nghiêm ngặt có thể loại một số trang nếu API trả ID của parent
  hoặc configurable product thay cho simple product. Quy tắc này cần đối chiếu
  với JSON thật.

## 16. Đề xuất thứ tự chỉnh sửa

1. Điền đúng MongoDB endpoint và `authSource`.
2. Chạy discovery trên môi trường test hoặc giới hạn dữ liệu thủ công.
3. Xem `python main.py stats` để kiểm tra số product distinct và candidate URL.
4. Crawl một số lượng nhỏ bằng cách tạm giảm dữ liệu queue hoặc thêm tùy chọn
   limit nếu cần.
5. Mở vài dòng output để đối chiếu field với JSON thật.
6. Sau khi xác nhận đúng dữ liệu, điều chỉnh concurrency và chạy production.

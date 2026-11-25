# VAFT - Đặc tả API & Tập lệnh (API Reference & Command Specification)

Hệ thống **VAFT** cung cấp giao diện lập trình HTTP RESTful thông qua cổng quản trị Admin (Mặc định: Node 1 là `8001`, Node 2 là `8002`, Node 3 là `8003`).

---

## 1. Tập Lệnh Cơ Sở Dữ Liệu (Client Commands)

Gửi câu lệnh qua endpoint:
* **URL:** `POST /api/command`
* **Content-Type:** `text/plain` hoặc `application/json` (Payload là chuỗi command thô)

### Bảng tra cứu cú pháp:

| Lệnh | Cú pháp | Loại | Mô tả | Giá trị trả về khi thành công |
| :--- | :--- | :---: | :--- | :--- |
| **`SET`** | `SET <key> <value>`<br>*(Hỗ trợ cả `SET <key>=<value>`)* | Ghi | Lưu hoặc cập nhật giá trị của một khóa. | `"OK"` |
| **`GET`** | `GET <key>` | Đọc | Lấy giá trị của một khóa từ State Machine. | `<value>` |
| **`DEL`** | `DEL <key>` | Ghi | Xóa một khóa khỏi State Machine. | `"OK"` |
| **`CAS`** | `CAS <key> <oldValue> <newValue>` | Ghi | Cập nhật nguyên tử: Chỉ đổi sang `newValue` nếu giá trị hiện tại khớp `oldValue`. | `"OK"` |
| **`EXISTS`**| `EXISTS <key>` | Đọc | Kiểm tra khóa có tồn tại hay không. | `"TRUE"` hoặc `"FALSE"` |
| **`KEYS`** | `KEYS`<br>`KEYS <pattern>` | Đọc | Liệt kê toàn bộ khóa hoặc lọc theo tiền tố (Ví dụ: `KEYS user:*`). | `"key1, key2, ..."` |

---

## 2. Đặc tả Chi tiết các HTTP Endpoint

### 2.1. Thực thi Lệnh Client (`/api/command`)

#### Phương thức: `POST` (Dành cho cả đọc và ghi)
* **Request Body:** Chuỗi lệnh thô (ví dụ: `SET user:100 NguyenVanA`).
* **Ví dụ cURL:**
  ```bash
  curl -X POST http://localhost:8001/api/command -d "SET user:100 NguyenVanA"
  ```
* **Response (Thành công - 200 OK):**
  ```json
  {
    "accepted": true,
    "status": "COMMITTED",
    "leaderId": 1,
    "logIndex": 4,
    "logTerm": 1,
    "stateMachineOutput": "OK"
  }
  ```

#### Phương thức: `GET` (Truy vấn nhanh tiện lợi từ trình duyệt)
Hỗ trợ tham số truy vấn `cmd` hoặc `key`:
* **Ví dụ 1 (Đọc theo key):**
  ```bash
  curl "http://localhost:8001/api/command?key=user:100"
  ```
* **Ví dụ 2 (Quét danh sách key):**
  ```bash
  curl "http://localhost:8001/api/command?cmd=KEYS+user:*"
  ```
* **Response (200 OK):**
  ```json
  {
    "accepted": true,
    "status": "SUCCESS",
    "leaderId": 1,
    "logIndex": -1,
    "logTerm": 1,
    "stateMachineOutput": "user:100"
  }
  ```

---

### 2.2. Truy vấn Trạng thái Node (`GET /api/status`)

Trả về ảnh chụp nhanh (Snapshot) toàn diện về vai trò, nhiệm kỳ, log, và toàn bộ dữ liệu trong State Machine của Node.

* **Ví dụ cURL:**
  ```bash
  curl http://localhost:8001/api/status
  ```
* **Response (200 OK):**
  ```json
  {
    "nodeId": "1",
    "role": "LEADER",
    "currentTerm": 1,
    "votedFor": "1",
    "leaderId": 1,
    "commitIndex": 4,
    "log": [
      { "index": 0, "term": 0, "command": null },
      { "index": 1, "term": 1, "command": "SET user:1 Tuan" },
      { "index": 2, "term": 1, "command": "SET user:2 Nam" },
      { "index": 3, "term": 1, "command": "CAS user:1 Tuan DucTuan" },
      { "index": 4, "term": 1, "command": "DEL user:2" }
    ],
    "stateMachine": {
      "user:1": "DucTuan"
    },
    "networkIsolated": false
  }
  ```

---

### 2.3. Lấy Danh sách Lịch sử Log (`GET /api/logs`)

Trả về toàn bộ các `LogEntry` hiện có trong Write-Ahead Log (WAL) của node.

* **Ví dụ cURL:**
  ```bash
  curl http://localhost:8001/api/logs
  ```
* **Response (200 OK):**
  ```json
  [
    { "index": 0, "term": 0, "command": null },
    { "index": 1, "term": 1, "command": "SET user:1 Tuan" }
  ]
  ```

---

### 2.4. Chaos API: Cô lập Mạng (`POST /api/chaos/isolate`)

Mô phỏng sự cố đứt cáp mạng hoặc sập switch: node sẽ bị cô lập hoàn toàn, không thể gửi nhận gói tin với các node khác trong cụm và từ chối client command.

* **Ví dụ cURL:**
  ```bash
  curl -X POST http://localhost:8001/api/chaos/isolate
  ```
* **Response (200 OK):**
  ```json
  {
    "status": "ISOLATED",
    "nodeId": 1
  }
  ```

---

### 2.5. Chaos API: Phục hồi Mạng (`POST /api/chaos/heal`)

Khôi phục kết nối mạng cho node sau khi bị cô lập, cho phép node tái hòa nhập cụm và đồng bộ lại log.

* **Ví dụ cURL:**
  ```bash
  curl -X POST http://localhost:8001/api/chaos/heal
  ```
* **Response (200 OK):**
  ```json
  {
    "status": "HEALED",
    "nodeId": 1
  }
  ```

---

## 3. Bảng Mã Lỗi & Phản Hồi Đặc Biệt

| Mã HTTP | Trạng thái (`status`) | Ý nghĩa & Cách xử lý |
| :---: | :--- | :--- |
| **`200`** | `"COMMITTED"` | Lệnh ghi đã được Quorum chấp thuận và áp dụng thành công vào State Machine. |
| **`200`** | `"SUCCESS"` | Lệnh đọc (GET, KEYS, EXISTS) đã trả về kết quả chính xác từ bộ nhớ. |
| **`400`** | `"NOT_LEADER"` | Node nhận được lệnh **không phải là Leader**. Client cần đọc trường `leaderId` trong response để tự động chuyển tiếp request sang Leader. |
| **`400`** | `"MALFORMED_COMMAND: ..."` | Cú pháp lệnh gửi lên không hợp lệ (không thuộc SET, GET, DEL, CAS, EXISTS, KEYS). |
| **`400`** | `"EXECUTION_ERROR: ..."` | Lỗi nghiệp vụ phát sinh từ State Machine (ví dụ: `CAS_FAILED` do sai giá trị kỳ vọng, hoặc xóa key không tồn tại). |
| **`503`** | `{"error": "Node is isolated..."}` | Node đang ở chế độ cô lập mạng (Chaos Mode) nên từ chối xử lý lệnh từ bên ngoài. |

---

## 4. Đặc tả Cấu Hình Cụm (`cluster.yaml`)

File cấu hình tại `src/main/resources/cluster.yaml` điều khiển toàn bộ hành vi mạng, thời gian bầu cử và ngưỡng lưu Snapshot:

```yaml
nodes:
  - id: 1
    host: node1
    port: 9001
    adminPort: 8001 
  - id: 2
    host: node2
    port: 9002
    adminPort: 8002
  - id: 3
    host: node3
    port: 9003
    adminPort: 8003

electionConfig:
  heartbeatIntervalMs: 150      # Khoảng thời gian Leader gửi Heartbeat (ms)
  minTimeoutMs: 400             # Thời gian timeout bầu cử tối thiểu (ms)
  maxTimeoutMs: 900             # Thời gian timeout bầu cử tối đa (ms)

networkConfig:
  connectTimeoutMs: 2000        # Timeout thiết lập kết nối TCP Socket (ms)
  requestTimeoutMs: 3000        # Timeout chờ phản hồi RPC (ms)

snapshotConfig:
  snapshotThresholdEntries: 50  # Số log entries chênh lệch giữa lastApplied và lastSnapshotIndex để kích hoạt Auto-Snapshot
```

### File Dữ liệu Lưu trữ Vật lý:
Mỗi node khi chạy sẽ tự động tạo thư mục `data/` và file SQLite tương ứng theo ID:
* `data/node-1.db` (kèm `node-1.db-wal` và `node-1.db-shm` khi đang chạy).
* `data/node-2.db`
* `data/node-3.db`

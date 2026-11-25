# VAFT - Distributed Consensus Engine & Replicated Key-Value Store

<p align="center">
  <b>A High-Performance, Fault-Tolerant Distributed Consensus System based on Raft Protocol with SQLite WAL Persistence in Java 21</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-21-orange.svg" alt="Java 21" />
  <img src="https://img.shields.io/badge/Raft-Consensus-blue.svg" alt="Raft Consensus" />
  <img src="https://img.shields.io/badge/Storage-SQLite%20WAL-lightgrey.svg" alt="SQLite WAL" />
  <img src="https://img.shields.io/badge/Networking-Custom%20TCP%20Binary-brightgreen.svg" alt="Custom TCP" />
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED.svg" alt="Docker Ready" />
  <img src="https://img.shields.io/badge/Tests-15%20Passing-success.svg" alt="Tests Passing" />
</p>

---

## 📖 Giới thiệu (Overview)

**VAFT** (Vietnamese Raft) là một hệ thống phân tán chịu lỗi (Distributed Fault-Tolerant System) được xây dựng dựa trên thuật toán đồng thuận **Raft (Raft Consensus Algorithm)** của Diego Ongaro và John Ousterhout.

Dự án cung cấp một **Cơ sở dữ liệu Key-Value phân tán (Replicated KV Store)** hỗ trợ tính nhất quán mạnh (Strong Consistency), tự động bầu Leader khi có sự cố (Leader Election), nhân bản log tuần tự (Log Replication), **lưu trữ bền vững xuống đĩa thông qua SQLite WAL Mode**, **tự động chụp ảnh trạng thái (Snapshotting)** và tích hợp sẵn công cụ **Chaos Engineering (Network Partition)** để kiểm thử khả năng chịu lỗi mạng (Split-Brain).

---

## 🌟 Điểm nổi bật của dự án (Key Features)

1. **Thuật toán Đồng thuận Raft hoàn chỉnh:**
   * **Leader Election:** Bầu cử Leader tự động với bộ đếm thời gian ngẫu nhiên (Randomized Election Timeout: 400ms - 900ms) chống xung đột phiếu bầu (Split Votes).
   * **Log Replication & Quorum Commitment:** Leader gửi `AppendEntries` định kỳ (150ms Heartbeat) và commit các entry khi đạt đa số ($N/2 + 1$).
   * **Log Matching & Conflict Resolution:** Tự động sửa chữa và ghi đè các log bị lệch của Follower khi có sự cố mạng.

2. **Tầng Lưu Trữ Bền Vững SQLite WAL & Tự Động Snapshot (Persistence & Log Compaction):**
   * **SQLite Engine (`SQLitePersistenceStorage`):** Lưu trữ metadata (`currentTerm`, `votedFor`) và Write-Ahead Log (`raft_log`) vào cơ sở dữ liệu nhúng SQLite local (`data/node-<id>.db`).
   * **Tối ưu HĐH với SQLite WAL Mode:** Bật `PRAGMA journal_mode = WAL;` và `PRAGMA synchronous = NORMAL;`, cho phép ghi tuần tự hiệu năng cao, chống hỏng dữ liệu khi sập nguồn đột ngột.
   * **Tự động Snapshot (Log Compaction):** Cấu hình `snapshotThresholdEntries: 50`. Khi số lượng log đã apply vượt ngưỡng, hệ thống tự động lưu bản chụp `RaftSnapshot` và metadata vào SQLite (`raft_snapshot_meta`, `raft_snapshot_kv`).
   * **Quy trình Phục hồi (Crash Recovery):** Khi Node khởi động lại, hệ thống tự động nạp lại Snapshot mới nhất vào `StateMachine`, khôi phục term/vote vào `RaftState`, và nạp lại log từ SQLite mà không làm mất dữ liệu.

3. **Custom Binary Protocol (Giao thức mạng nhị phân hiệu năng cao):**
   * Sử dụng tầng mạng TCP Socket thuần với Header Magic `0x5641` (`"VA"`).
   * Tự thiết kế bộ mã hóa `PacketCodec` truyền nhận nhị phân trực tiếp (Zero-reflection), tối ưu hóa kích thước gói tin và tốc độ truyền tải so với JSON/XML.

4. **Replicated Key-Value Database & State Machine:**
   * Hỗ trợ đầy đủ các thao tác: `SET`, `GET`, `DEL`, `CAS` (Compare-And-Swap), `EXISTS`, và `KEYS` (Prefix Scan).
   * **Tối ưu hóa Luồng Xử Lý (Read/Write Separation):**
     * **Write Path (`SET`, `DEL`, `CAS`):** Bắt buộc ghi Raft WAL (SQLite), đạt Quorum đa số trước khi Apply vào State Machine.
     * **Read Path (`GET`, `KEYS`, `EXISTS`):** Đọc trực tiếp từ State Machine in-memory tại Leader, **không ghi log Raft** giúp giảm tải I/O đĩa và đưa độ trễ về dưới 1ms.

5. **Tích hợp Chaos Engineering & Admin Dashboard API:**
   * Cung cấp HTTP REST API điều khiển trạng thái từng node.
   * Giả lập cô lập node (`/api/chaos/isolate`) và phục hồi mạng (`/api/chaos/heal`) để kiểm thử khả năng chống phân vùng mạng (Network Partition / Split-brain).
   * Endpoint truy vấn trạng thái toàn cụm `/api/status` và lịch sử log `/api/logs`.

6. **Hiện đại hóa với Java 21:**
   * Tận dụng **Java Records** cho bất biến dữ liệu giao thức (`LogEntry`, `PersistentState`, `RaftSnapshot`, `Command`, `ClientCommandResult`).
   * Sử dụng **Virtual Threads** (`Executors.newVirtualThreadPerTaskExecutor()`) cho HTTP Server quản trị.

---

## 🏛️ Kiến trúc hệ thống (Architecture)

```text
                                  ┌───────────────────────────────┐
                                  │      Client / Dashboard       │
                                  └───────────────┬───────────────┘
                                                  │ HTTP (Port 8001/8002/8003)
                                                  ▼
                                  ┌───────────────────────────────┐
                                  │     DashboardAdminServer      │
                                  └───────────────┬───────────────┘
                                                  │
                 ┌────────────────────────────────┴───────────────────────────────┐
                 │                                                                │
                 ▼ (Read Path: GET, KEYS)                                         ▼ (Write Path: SET, DEL, CAS)
   ┌───────────────────────────┐                                    ┌───────────────────────────┐
   │       StateMachine        │                                    │    ReplicationService     │
   │  (In-Memory KV Store)     │◄─────────[Apply Committed]─────────│  - Manage Client Futures  │
   │  ConcurrentHashMap        │                                    │  - Check Quorum Advance   │
   │  Snapshot Data Restore    │                                    │  - Trigger Auto Snapshot  │
   └─────────────┬─────────────┘                                    └─────────────┬─────────────┘
                 │                                                                │
                 │                                                                ▼
                 │                                                  ┌───────────────────────────┐
                 │                                                  │      RaftLogManager       │
                 │                                                  │  - In-Memory Log Cache    │
                 │                                                  │  - Invariant Verification │
                 │                                                  └─────────────┬─────────────┘
                 │                                                                │
                 └───────────────────────────────┬────────────────────────────────┘
                                                 ▼
                                   ┌─────────────────────────────┐
                                   │  SQLitePersistenceStorage   │
                                   │  - PRAGMA journal_mode=WAL  │
                                   │  - raft_meta, raft_log      │
                                   │  - snapshot_meta, snap_kv   │
                                   │  File: data/node-<id>.db    │
                                   └──────────────┬──────────────┘
                                                  │
                                                  ▼
                                   ┌─────────────────────────────┐
                                   │       TcpRaftTransport      │
                                   │  - PacketCodec (Magic 0x5641│
                                   │  - PartitionTable (Chaos)   │
                                   └──────────────┬──────────────┘
                                                  │ Custom TCP (Port 9001/9002/9003)
                                                  ▼
                                    [Các Node khác trong Cụm]
```

---

## 📂 Cấu trúc Thư mục (Project Layout)

```text
vaft/
├── pom.xml                               # Cấu hình Maven (Java 21, SQLite JDBC, JUnit 5, Jackson, SLF4J)
├── Dockerfile                            # Multi-stage Docker build
├── docker-compose.yml                    # Thiết lập cụm 3 node Raft
├── README.md                             # Tài liệu tổng quan dự án
├── docs/                                 # Tài liệu kỹ thuật chi tiết
│   ├── ARCHITECTURE.md                   # Kiến trúc, giao thức mạng, SQLite Engine & Snapshotting
│   └── API_REFERENCE.md                  # Đặc tả HTTP REST API & tập lệnh Database
└── src/
    ├── main/
    │   ├── resources/
    │   │   └── cluster.yaml              # Cấu hình cụm node, cổng, timeout, snapshot threshold
    │   └── java/vaft/
    │       ├── App.java                  # Điểm khởi chạy chương trình & Phục hồi từ SQLite
    │       ├── bootstrap/                # Quản lý vòng đời Node (RaftNode)
    │       ├── config/                   # Đọc cấu hình YAML (ClusterConfig, NodeInfo, SnapshotConfig)
    │       ├── core/                     # Quản lý trạng thái Node (RaftState, NodeRole)
    │       ├── dashboard/                # HTTP Admin Server, REST API, JSON Serialization
    │       ├── election/                 # Thuật toán bầu cử Leader (ElectionService)
    │       ├── protocol/                 # Mô hình gói tin (RequestVote, AppendEntries, Envelopes)
    │       ├── replication/              # Thuật toán nhân bản log, commit Quorum & Auto-Snapshot
    │       ├── state/                    # State Machine: Key-Value Database & Parser
    │       ├── storage/                  # Tầng lưu trữ bền vững (PersistenceStorage, RaftSnapshot)
    │       │   └── impl/
    │       │       └── SQLitePersistenceStorage.java  # Triển khai SQLite WAL mode
    │       ├── transport/                # Tầng mạng TCP Socket, PacketCodec & Chaos Table
    │       └── utils/                    # Tiện ích phân tích tham số dòng lệnh
    └── test/java/vaft/                   # Bộ Unit Tests (JUnit 5)
        ├── replication/ReadWritePathTest.java
        ├── state/StateMachineTest.java
        └── storage/SQLitePersistenceStorageTest.java
```

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy (Getting Started)

### Yêu cầu tiên quyết (Prerequisites)
* **Java Development Kit (JDK):** Phiên bản **21** trở lên.
* **Apache Maven:** Phiên bản **3.8+**.
* **Docker & Docker Compose:** (Nếu muốn chạy cụm container).

---

### Cách 1: Khởi chạy Cụm 3 Node bằng Docker Compose (Khuyên dùng)

Cách nhanh nhất để quan sát cụm Raft 3 node tự động bầu Leader:

```bash
# 1. Di chuyển vào thư mục dự án
cd vaft

# 2. Build image và khởi động cụm 3 node
docker compose up --build
```

**Bảng cổng dịch vụ sau khi khởi động:**

| Node | Hostname | Cổng Raft TCP (Nội bộ) | Cổng HTTP Admin (Host) | File Dữ Liệu SQLite |
| :---: | :---: | :---: | :---: | :---: |
| **Node 1** | `node1` | `9001` | [`http://localhost:8001`](http://localhost:8001) | `data/node-1.db` |
| **Node 2** | `node2` | `9002` | [`http://localhost:8002`](http://localhost:8002) | `data/node-2.db` |
| **Node 3** | `node3` | `9003` | [`http://localhost:8003`](http://localhost:8003) | `data/node-3.db` |

---

### Cách 2: Biên dịch và chạy thủ công (Local Maven)

#### 1. Đóng gói file JAR:
```bash
mvn clean package -DskipTests
```
File JAR kèm toàn bộ dependencies sẽ được sinh ra tại:
`target/vaft-1.0-jar-with-dependencies.jar`.

#### 2. Khởi chạy 3 tiến trình ở 3 terminal riêng biệt:
```bash
# Terminal 1 (Node 1)
java -jar target/vaft-1.0-jar-with-dependencies.jar --nodeId=1 --config=src/main/resources/cluster.yaml

# Terminal 2 (Node 2)
java -jar target/vaft-1.0-jar-with-dependencies.jar --nodeId=2 --config=src/main/resources/cluster.yaml

# Terminal 3 (Node 3)
java -jar target/vaft-1.0-jar-with-dependencies.jar --nodeId=3 --config=src/main/resources/cluster.yaml
```

---

## 🛠️ Hướng dẫn Sử dụng & Thao tác với Database

Hệ thống cung cấp HTTP endpoint `/api/command` tại cổng Admin của mỗi node để client gửi lệnh đọc/ghi.

### 1. Ghi dữ liệu (Write Operations)
Lệnh ghi bắt buộc phải gửi đến **Leader hiện tại** (nếu gửi đến Follower, hệ thống sẽ trả về lỗi `NOT_LEADER` kèm `leaderId` để redirect):

* **Lưu / Cập nhật (`SET`):**
  ```bash
  curl -X POST http://localhost:8001/api/command -d "SET user:1 Tuan"
  ```
  *Kết quả:* `{"accepted":true,"status":"COMMITTED","leaderId":1,"logIndex":1,"logTerm":1,"stateMachineOutput":"OK"}`

* **So sánh và cập nhật nguyên tử (`CAS` - Compare-And-Swap):**
  ```bash
  # Cú pháp: CAS <key> <expectedOldValue> <newValue>
  curl -X POST http://localhost:8001/api/command -d "CAS user:1 Tuan DucTuan"
  ```

* **Xóa dữ liệu (`DEL`):**
  ```bash
  curl -X POST http://localhost:8001/api/command -d "DEL user:1"
  ```

---

### 2. Đọc dữ liệu (Read Operations - Tối ưu trễ thấp)
Các lệnh đọc không làm thay đổi trạng thái, được phục vụ trực tiếp từ bộ nhớ RAM của Leader:

* **Lấy giá trị theo Key (`GET`):**
  ```bash
  # Qua POST
  curl -X POST http://localhost:8001/api/command -d "GET user:1"

  # Hoặc qua GET URL nhanh:
  curl "http://localhost:8001/api/command?key=user:1"
  ```

* **Kiểm tra tồn tại (`EXISTS`):**
  ```bash
  curl -X POST http://localhost:8001/api/command -d "EXISTS user:1"
  ```

* **Xem toàn bộ danh sách Key hoặc quét theo tiền tố (`KEYS`):**
  ```bash
  # Lấy toàn bộ keys
  curl "http://localhost:8001/api/command?cmd=KEYS"

  # Quét theo prefix
  curl "http://localhost:8001/api/command?cmd=KEYS+user:*"
  ```

---

## 💾 Cơ chế Lưu trữ Bền vững SQLite & Snapshotting

Hệ thống sử dụng bảng dữ liệu SQLite nhúng trong file `data/node-<id>.db`:

1. **Bảng `raft_meta`:** Lưu trữ `current_term` và `voted_for`. Tự động cập nhật mỗi khi term hoặc vote thay đổi.
2. **Bảng `raft_log`:** Lưu trữ tuần tự các `LogEntry` (`log_index`, `term`, `command`). Hỗ trợ batch insert và truncate khi có xung đột.
3. **Bảng `raft_snapshot_meta`:** Lưu `last_included_index` và `last_included_term`.
4. **Bảng `raft_snapshot_kv`:** Lưu toàn bộ các cặp key-value của State Machine tại thời điểm chụp snapshot.
5. **Auto-Snapshotting:** Cứ mỗi **50 log entries** được commit và apply (hoặc theo cấu hình `snapshotConfig.snapshotThresholdEntries` trong `cluster.yaml`), node sẽ tự động chụp ảnh `StateMachine` lưu vào SQLite và cập nhật `lastSnapshotIndex`.

---

## 🧪 Kịch bản Thử nghiệm Chaos (Network Partition)

Hệ thống cho phép bạn mô phỏng kịch bản rớt mạng thực tế bằng các API tích hợp:

### 1. Kiểm tra trạng thái hiện tại của Node
```bash
curl http://localhost:8001/api/status
```
*Kết quả trả về JSON chi tiết:* `role` (LEADER / FOLLOWER / CANDIDATE), `currentTerm`, `commitIndex`, `log`, và toàn bộ `stateMachine`.

### 2. Mô phỏng cô lập Leader (Network Partition)
Giả sử Node 1 đang là Leader, ta cô lập Node 1:
```bash
curl -X POST http://localhost:8001/api/chaos/isolate
```
* **Hiện tượng xảy ra:**
  * Node 2 và Node 3 không nhận được Heartbeat từ Node 1.
  * Sau `electionTimeout` (ngẫu nhiên 400 - 900ms), Node 2 hoặc Node 3 sẽ tăng `currentTerm` và tự ứng cử.
  * Đạt Quorum đa số (2/3 phiếu), một Leader mới được bầu ra thành công!
  * Node 1 bị cô lập sẽ từ chối các yêu cầu từ Client với mã `503 Service Unavailable`.

### 3. Phục hồi mạng (Heal Partition)
Kết nối lại Node 1 vào mạng:
```bash
curl -X POST http://localhost:8001/api/chaos/heal
```
* **Hiện tượng xảy ra:**
  * Node 1 nhận Heartbeat từ Leader mới với Term cao hơn -> Tự động thoái vị về `FOLLOWER`.
  * Node 1 đồng bộ lại các log bị thiếu từ Leader mới và cập nhật State Machine.

---

## 🔬 Chạy Kiểm thử Tự động (Unit Tests)

Dự án đi kèm bộ kiểm thử tự động toàn diện được viết bằng **JUnit 5** (15 tests, 100% pass):
```bash
mvn clean test
```
* **`StateMachineTest` (6 tests):** Kiểm thử tính đúng đắn của parser lệnh, `SET`, `GET`, `DEL`, `CAS`, `EXISTS`, `KEYS`, và khôi phục Snapshot.
* **`ReadWritePathTest` (4 tests):** Kiểm thử phân luồng Read Path không sinh log WAL, Write Path cam kết qua Quorum, và cơ chế chuyển hướng `NOT_LEADER`.
* **`SQLitePersistenceStorageTest` (5 tests):** Kiểm thử đọc/ghi metadata, batch append log, truncate log khi bị ghi đè, lưu/nạp Snapshot và tự động kích hoạt Snapshot sau 50 entries.

---

## 📚 Tài liệu bổ sung (Further Documentation)
* 📐 [Chi tiết Kiến trúc Hệ thống, SQLite WAL & Snapshotting (ARCHITECTURE.md)](docs/ARCHITECTURE.md)
* 📡 [Đặc tả HTTP REST API & Danh mục Mã lỗi (API_REFERENCE.md)](docs/API_REFERENCE.md)

---

## 👥 Nhóm Tác Giả & Giấy Phép
* **Đồ án:** PBL4 - Hệ thống Tính toán Phân tán & Mạng Máy Tính.
* **Ngôn ngữ:** Java 21 / Docker.
* **Giấy phép:** MIT License.

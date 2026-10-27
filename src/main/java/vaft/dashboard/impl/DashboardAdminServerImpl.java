package vaft.dashboard.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.RaftState;
import vaft.dashboard.DashboardAdminServer;
import vaft.dashboard.NodeStatusSnapshot;
import vaft.protocol.ClientCommandResult;
import vaft.protocol.LogEntry;
import vaft.replication.RaftLogView;
import vaft.replication.ReplicationService;
import vaft.state.StateMachine;
import vaft.transport.NetworkPartitionTable;

public class DashboardAdminServerImpl implements DashboardAdminServer {

    private final int port;
    private final NodeInfo self;
    private final ClusterConfig clusterConfig;
    private final RaftState raftState;
    private final RaftLogView logView;
    private final StateMachine stateMachine;
    private final NetworkPartitionTable partitionTable;
    private final ReplicationService replicationService;

    private HttpServer server;

    public DashboardAdminServerImpl(
            int port,
            NodeInfo self,
            ClusterConfig clusterConfig,
            RaftState raftState,
            RaftLogView logView,
            StateMachine stateMachine,
            NetworkPartitionTable partitionTable,
            ReplicationService replicationService) {
        this.port = port;
        this.self = self;
        this.clusterConfig = clusterConfig;
        this.raftState = raftState;
        this.logView = logView;
        this.stateMachine = stateMachine;
        this.partitionTable = partitionTable;
        this.replicationService = replicationService;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            // Các Endpoint phục vụ Web UI và API
            server.createContext("/", this::handleWebUi);
            server.createContext("/status", this::handleGetStatus);
            server.createContext("/api/status", this::handleGetStatus);
            server.createContext("/chaos/isolate", this::handleIsolate);
            server.createContext("/chaos/heal", this::handleHeal);
            server.createContext("/client/command", this::handleClientCommand);

            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            System.out.println("Dashboard server running at http://localhost:" + port);
        } catch (IOException e) {
            System.err.println("Failed to start dashboard server: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handleGetStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }

        NodeStatusSnapshot snapshot = buildSnapshot();
        sendResponse(exchange, 200, "application/json", snapshot.toJson());
    }

    private void handleIsolate(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }

        if (partitionTable != null) {
            partitionTable.isolateNode(self.getId());
        }
        sendResponse(exchange, 200, "application/json", "{\"status\":\"ISOLATED\"}");
    }

    private void handleHeal(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }

        if (partitionTable != null) {
            partitionTable.healNode(self.getId());
        }
        sendResponse(exchange, 200, "application/json", "{\"status\":\"CONNECTED\"}");
    }

    private void handleClientCommand(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }

        String command = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (command.isEmpty()) {
            sendResponse(exchange, 400, "application/json", "{\"error\":\"Command is empty\"}");
            return;
        }

        if (replicationService == null) {
            sendResponse(exchange, 503, "application/json", "{\"error\":\"ReplicationService unavailable\"}");
            return;
        }

        try {
            CompletableFuture<ClientCommandResult> future = replicationService.submitClientCommand(command);
            ClientCommandResult result = future.get(3, TimeUnit.SECONDS);
            String json = String.format(
                    "{\"accepted\":%b,\"status\":\"%s\",\"leaderId\":%d,\"logIndex\":%d,\"logTerm\":%d}",
                    result.accepted(), result.status(), result.leaderId(), result.logIndex(), result.logTerm());
            sendResponse(exchange, 200, "application/json", json);
        } catch (Exception e) {
            sendResponse(exchange, 500, "application/json", "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private NodeStatusSnapshot buildSnapshot() {
        return raftState.withLock(() -> {
            boolean isolated = partitionTable != null && partitionTable.isBlocked(self.getId(), -1);
            List<LogEntry> logs = logView != null ? logView.getAllEntriesSnapshot() : Collections.emptyList();
            Map<String, String> fsmData = stateMachine != null ? stateMachine.getSnapshotData()
                    : Collections.emptyMap();
            List<NodeInfo> allNodes = clusterConfig != null ? clusterConfig.getNodes() : List.of(self);
            Map<Integer, Integer> matchIndexes = raftState.getMatchIndex();

            return new NodeStatusSnapshot(
                    self.getId(),
                    raftState.getRole(),
                    raftState.getCurrentTerm(),
                    raftState.getVotedFor(),
                    raftState.getLeaderId(),
                    raftState.getCommitIndex(),
                    logs,
                    fsmData,
                    isolated,
                    allNodes,
                    matchIndexes);
        });
    }

    private void handleWebUi(HttpExchange exchange) throws IOException {
        if (!"/".equals(exchange.getRequestURI().getPath())) {
            sendResponse(exchange, 404, "text/plain", "Not Found");
            return;
        }

        String html = """
                <!DOCTYPE html>
                <html lang="vi">
                <head>
                    <meta charset="UTF-8">
                    <title>VAFT Cluster Topology & Dashboard</title>
                    <style>
                        :root {
                            --bg: #0b1329;
                            --card-bg: #152238;
                            --border: #1e3a5f;
                            --primary: #38bdf8;
                            --leader: #22c55e;
                            --follower: #3b82f6;
                            --candidate: #f59e0b;
                            --isolated: #ef4444;
                            --text: #f1f5f9;
                            --text-dim: #94a3b8;
                        }
                        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }
                        .container { max-width: 1050px; margin: 0 auto; }
                        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--border); padding-bottom: 15px; margin-bottom: 25px; }
                        .header h1 { margin: 0; font-size: 26px; color: var(--primary); display: flex; align-items: center; gap: 10px; }
                        .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 8px 16px rgba(0,0,0,0.3); }
                        .card-title { font-size: 17px; font-weight: bold; margin-bottom: 15px; display: flex; align-items: center; justify-content: space-between; color: var(--primary); }

                        /* SƠ ĐỒ TOPO MẠNG LƯỚI (MÔ HÌNH HÌNH SAO / HIERARCHY STAR TOPOLOGY) */
                        .topo-container { position: relative; background: #070d1e; border: 1px dashed var(--border); border-radius: 12px; padding: 25px 20px; min-height: 290px; display: flex; flex-direction: column; justify-content: space-between; align-items: center; overflow: hidden; }
                        .topo-svg-layer { position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 1; }
                        .topo-tier { display: flex; justify-content: space-around; width: 100%; position: relative; z-index: 2; }
                        .topo-tier-leader { justify-content: center; margin-bottom: 45px; }
                        .topo-tier-followers { justify-content: space-evenly; max-width: 850px; }
                        .topo-node { position: relative; width: 155px; padding: 12px 10px; border-radius: 12px; background: var(--card-bg); text-align: center; border: 2px solid var(--border); transition: all 0.3s; box-shadow: 0 4px 12px rgba(0,0,0,0.4); }
                        .topo-node.is-leader { border-color: var(--leader); box-shadow: 0 0 25px rgba(34, 197, 94, 0.45); background: #132838; }
                        .topo-node.is-isolated { border-color: var(--isolated); box-shadow: 0 0 20px rgba(239, 68, 68, 0.45); filter: grayscale(40%); }
                        .node-avatar { font-size: 26px; margin-bottom: 3px; }
                        .node-name { font-weight: bold; font-size: 14px; }
                        .node-role-tag { font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: bold; margin-top: 4px; display: inline-block; }
                        .node-hb-box { background: rgba(0,0,0,0.3); border-radius: 6px; padding: 6px 8px; margin-top: 8px; font-size: 11px; text-align: left; }
                        .node-hb-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px; }
                        .node-hb-label { color: var(--text-dim); }
                        .node-hb-val { font-weight: bold; color: var(--primary); }
                        .pulse-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
                        .pulse-green { background: var(--leader); box-shadow: 0 0 6px var(--leader); animation: blink 1.2s infinite; }
                        .pulse-red { background: var(--isolated); box-shadow: 0 0 6px var(--isolated); }
                        @keyframes blink { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.3; transform: scale(0.8); } }

                        /* STATS GRID */
                        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 15px; }
                        .stat { background: #070d1e; padding: 12px; border-radius: 8px; text-align: center; border: 1px solid rgba(255,255,255,0.05); }
                        .stat-label { font-size: 12px; color: var(--text-dim); margin-bottom: 4px; text-transform: uppercase; }
                        .stat-value { font-size: 20px; font-weight: bold; }
                        .badge { padding: 4px 10px; border-radius: 12px; font-size: 13px; font-weight: bold; display: inline-block; }
                        .badge-LEADER { background: var(--leader); color: #000; }
                        .badge-FOLLOWER { background: var(--follower); color: #fff; }
                        .badge-CANDIDATE { background: var(--candidate); color: #000; }
                        .badge-ISOLATED { background: var(--isolated); color: #fff; }

                        /* CONTROLS & TABLES */
                        .btn { padding: 9px 16px; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; transition: 0.2s; margin-right: 8px; font-size: 13px; }
                        .btn-danger { background: var(--isolated); color: white; }
                        .btn-success { background: var(--leader); color: white; }
                        .btn-primary { background: var(--primary); color: #000; }
                        .btn:hover { opacity: 0.85; transform: translateY(-1px); }
                        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
                        th, td { border: 1px solid var(--border); padding: 8px 12px; text-align: left; font-size: 13px; }
                        th { background: #070d1e; color: var(--primary); }
                        input[type="text"] { flex: 1; padding: 10px; border-radius: 6px; border: 1px solid var(--border); background: #070d1e; color: white; margin-right: 10px; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>🌐 VAFT Consensus Topology</h1>
                            <div>
                                <span id="selfNodeBadge" class="badge">Đang kết nối...</span>
                            </div>
                        </div>

                        <!-- 1. SƠ ĐỒ TOPO MẠNG ĐỘNG (STAR TOPOLOGY) -->
                        <div class="card">
                            <div class="card-title">
                                <span>📡 Cluster Live Topology (Mô hình hình Sao từ Leader tới Followers)</span>
                                <span style="font-size: 12px; font-weight: normal; color: var(--text-dim);">Tự động quét mỗi 1 giây</span>
                            </div>
                            <div class="topo-container" id="topoArea">
                                <svg class="topo-svg-layer" id="topoSvg"></svg>
                                <!-- Tầng 1: Leader đặt ở đỉnh trung tâm -->
                                <div class="topo-tier topo-tier-leader" id="leaderTier"></div>
                                <!-- Tầng 2: Các Follower trải đều bên dưới -->
                                <div class="topo-tier topo-tier-followers" id="followersTier"></div>
                            </div>
                            <div style="margin-top: 15px; display: flex; align-items: center; justify-content: space-between;">
                                <div>
                                    <strong>Chaos Testing trên Node này: </strong>
                                    <button class="btn btn-danger" onclick="chaos('isolate')">⚡ Cắt mạng (Isolate)</button>
                                    <button class="btn btn-success" onclick="chaos('heal')">🔌 Nối lại mạng (Heal)</button>
                                </div>
                                <span id="chaosStatusText" style="font-size: 13px; color: var(--text-dim);">Trạng thái: Bình thường</span>
                            </div>
                        </div>

                        <!-- 2. THỐNG KÊ CHI TIẾT CỦA NODE -->
                        <div class="card">
                            <div class="card-title">📊 Chi tiết trạng thái (Local Node State)</div>
                            <div class="grid">
                                <div class="stat"><div class="stat-label">Vai trò</div><div class="stat-value" id="role">-</div></div>
                                <div class="stat"><div class="stat-label">Nhiệm kỳ (Term)</div><div class="stat-value" id="term">-</div></div>
                                <div class="stat"><div class="stat-label">Leader hiện tại</div><div class="stat-value" id="leader">-</div></div>
                                <div class="stat"><div class="stat-label">Commit Index</div><div class="stat-value" id="commit">-</div></div>
                                <div class="stat"><div class="stat-label">Trạng thái mạng</div><div class="stat-value" id="network">-</div></div>
                            </div>
                        </div>

                        <!-- 3. GỬI LỆNH REPLICATION THỰC TẾ -->
                        <div class="card">
                            <div class="card-title">💬 Gửi lệnh Client (Test nhân bản dữ liệu FSM)</div>
                            <div style="display: flex;">
                                <input type="text" id="cmdInput" placeholder="Nhập lệnh Key-Value (VD: SET user=BKDN hoặc SET x=100)">
                                <button class="btn btn-primary" onclick="sendCommand()">Gửi lệnh</button>
                            </div>
                            <div id="cmdResult" style="margin-top: 10px; color: var(--primary); font-size: 13px;"></div>
                        </div>

                        <!-- 4. RAFT LOG TABLE -->
                        <div class="card">
                            <div class="card-title">📜 Raft Log Entries (Nhật ký phân tán)</div>
                            <table>
                                <thead><tr><th>Index</th><th>Term</th><th>Command</th></tr></thead>
                                <tbody id="logTable"><tr><td colspan="3">Chưa có log entry</td></tr></tbody>
                            </table>
                        </div>
                    </div>

                    <script>
                        let lastLeaderId = -1;

                        async function refresh() {
                            try {
                                const res = await fetch('/api/status');
                                const d = await res.json();

                                document.getElementById('selfNodeBadge').innerText = 'Local: Node ' + d.nodeId;
                                document.getElementById('role').innerHTML = `<span class="badge badge-${d.role}">${d.role}</span>`;
                                document.getElementById('term').innerText = d.currentTerm;
                                document.getElementById('leader').innerText = d.leaderId > 0 ? 'Node ' + d.leaderId : 'Đang bầu cử...';
                                document.getElementById('commit').innerText = d.commitIndex;
                                document.getElementById('network').innerHTML = d.networkIsolated
                                    ? '<span class="badge badge-ISOLATED">CÔ LẬP</span>'
                                    : '<span class="badge badge-LEADER">BÌNH THƯỜNG</span>';

                                document.getElementById('chaosStatusText').innerText = d.networkIsolated
                                    ? '⚠️ Node đang bị cắt cáp giả lập!'
                                    : '✅ Kết nối mạng TCP thông suốt';

                                // Render bảng Log
                                const tbody = document.getElementById('logTable');
                                if (d.log && d.log.length > 0) {
                                    tbody.innerHTML = d.log.map(e => `<tr><td>#${e.index}</td><td>Term ${e.term}</td><td><code>${e.command || '-'}</code></td></tr>`).join('');
                                }

                                // Render Sơ đồ Topo mạng các Node theo mô hình Hình Sao (Star)
                                renderTopology(d);

                            } catch (e) {
                                console.error('Fetch error:', e);
                            }
                        }

                        function renderTopology(data) {
                            const leaderTier = document.getElementById('leaderTier');
                            const followersTier = document.getElementById('followersTier');
                            const nodes = data.clusterNodes || [{ id: data.nodeId, host: 'localhost', port: 9001 }];

                            leaderTier.innerHTML = '';
                            followersTier.innerHTML = '';

                            const leaderNode = nodes.find(n => n.id === data.leaderId);
                            const peerNodes = nodes.filter(n => n.id !== data.leaderId);

                            // Nếu có Leader thì đưa Leader lên Tầng 1 (đỉnh), các Followers ở Tầng 2
                            if (leaderNode) {
                                leaderTier.appendChild(createNodeCard(leaderNode, data, true));
                                peerNodes.forEach(n => {
                                    followersTier.appendChild(createNodeCard(n, data, false));
                                });
                            } else {
                                // Khi đang bầu cử, tất cả xếp ở tầng dưới
                                nodes.forEach(n => {
                                    followersTier.appendChild(createNodeCard(n, data, false));
                                });
                            }

                            // Vẽ các đường dây kết nối hình sao từ Leader tới từng Follower
                            setTimeout(() => drawStarLinks(data, nodes), 40);
                        }

                        function createNodeCard(n, data, isLeader) {
                            const isSelf = (n.id === data.nodeId);
                            const isIsolated = (isSelf && data.networkIsolated);

                            let roleName = isLeader ? 'LEADER' : 'FOLLOWER';
                            if (isSelf && data.role === 'CANDIDATE') roleName = 'CANDIDATE';

                            let avatar = '💻';
                            if (isLeader) avatar = '👑';
                            if (isIsolated) avatar = '⚡';

                            // Lấy thông số matchIndex của node
                            const matchIdx = (data.matchIndex && data.matchIndex[n.id] !== undefined)
                                ? data.matchIndex[n.id]
                                : (isLeader ? data.commitIndex : data.commitIndex);

                            const card = document.createElement('div');
                            card.id = 'topoNode_' + n.id;
                            card.className = 'topo-node' + (isLeader ? ' is-leader' : '') + (isIsolated ? ' is-isolated' : '');

                            let hbContent = '';
                            if (isIsolated) {
                                hbContent = `
                                    <div class="node-hb-row" style="color:var(--isolated);">
                                        <span><span class="pulse-dot pulse-red"></span> Mạng:</span>
                                        <span class="node-hb-val" style="color:var(--isolated);">Bị cô lập</span>
                                    </div>
                                    <div class="node-hb-row">
                                        <span class="node-hb-label">Gói tin:</span>
                                        <span class="node-hb-val">0 pkt/s</span>
                                    </div>
                                `;
                            } else if (isLeader) {
                                hbContent = `
                                    <div class="node-hb-row">
                                        <span><span class="pulse-dot pulse-green"></span> Nhịp phát:</span>
                                        <span class="node-hb-val">150 ms/lần</span>
                                    </div>
                                    <div class="node-hb-row">
                                        <span class="node-hb-label">Commit:</span>
                                        <span class="node-hb-val">#${data.commitIndex}</span>
                                    </div>
                                `;
                            } else {
                                hbContent = `
                                    <div class="node-hb-row">
                                        <span><span class="pulse-dot pulse-green"></span> Nhịp nhận:</span>
                                        <span class="node-hb-val">150 ms</span>
                                    </div>
                                    <div class="node-hb-row">
                                        <span class="node-hb-label">MatchIndex:</span>
                                        <span class="node-hb-val">#${matchIdx}</span>
                                    </div>
                                `;
                            }

                            card.innerHTML = `
                                <div class="node-avatar">${avatar}</div>
                                <div class="node-name">Node ${n.id} ${isSelf ? '<small style="color:var(--primary); font-size:11px;"></small>' : ''}</div>
                                <span class="node-role-tag badge-${roleName}">${roleName}</span>
                                <div class="node-hb-box">
                                    ${hbContent}
                                </div>
                            `;
                            return card;
                        }

                        function drawStarLinks(data, nodes) {
                            const svg = document.getElementById('topoSvg');
                            const container = document.getElementById('topoArea');
                            svg.innerHTML = '';

                            if (data.leaderId <= 0) return;
                            const leaderEl = document.getElementById('topoNode_' + data.leaderId);
                            if (!leaderEl) return;

                            const cRect = container.getBoundingClientRect();
                            const lRect = leaderEl.getBoundingClientRect();

                            // Tọa độ điểm xuất phát từ cạnh đáy của Node Leader
                            const lx = lRect.left - cRect.left + lRect.width / 2;
                            const ly = lRect.top - cRect.top + lRect.height;

                            nodes.forEach(n => {
                                if (n.id === data.leaderId) return;
                                const peerEl = document.getElementById('topoNode_' + n.id);
                                if (!peerEl) return;

                                const pRect = peerEl.getBoundingClientRect();
                                // Tọa độ điểm đến tại đỉnh trên của Node Follower
                                const px = pRect.left - cRect.left + pRect.width / 2;
                                const py = pRect.top - cRect.top;

                                const isCut = (data.networkIsolated && (n.id === data.nodeId || data.leaderId === data.nodeId));
                                const strokeColor = isCut ? '#ef4444' : '#22c55e';
                                const strokeDash = isCut ? '6,6' : 'none';

                                // Đường cáp nối trực tiếp từ Leader tới từng Follower (Star Topology)
                                const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                                line.setAttribute('x1', lx);
                                line.setAttribute('y1', ly);
                                line.setAttribute('x2', px);
                                line.setAttribute('y2', py);
                                line.setAttribute('stroke', strokeColor);
                                line.setAttribute('stroke-width', '2.5');
                                line.setAttribute('stroke-dasharray', strokeDash);
                                line.setAttribute('opacity', isCut ? '0.4' : '0.85');
                                svg.appendChild(line);

                                // Luồng hạt Heartbeat bay liên tục từ Leader xuống từng Follower
                                if (!isCut) {
                                    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                                    circle.setAttribute('r', '4.5');
                                    circle.setAttribute('fill', '#38bdf8');
                                    circle.setAttribute('filter', 'drop-shadow(0 0 6px #38bdf8)');

                                    const animMotion = document.createElementNS('http://www.w3.org/2000/svg', 'animateMotion');
                                    animMotion.setAttribute('path', `M ${lx} ${ly} L ${px} ${py}`);
                                    animMotion.setAttribute('dur', '1.1s');
                                    animMotion.setAttribute('repeatCount', 'indefinite');

                                    circle.appendChild(animMotion);
                                    svg.appendChild(circle);
                                }
                            });
                        }

                        async function chaos(action) {
                            await fetch('/chaos/' + action, { method: 'POST' });
                            refresh();
                        }

                        async function sendCommand() {
                            const cmd = document.getElementById('cmdInput').value;
                            if (!cmd) return;
                            const resEl = document.getElementById('cmdResult');
                            resEl.innerText = 'Đang gửi lệnh tới cụm Raft...';
                            try {
                                const res = await fetch('/client/command', { method: 'POST', body: cmd });
                                const d = await res.json();
                                resEl.innerText = 'Kết quả: ' + JSON.stringify(d);
                                refresh();
                            } catch (e) {
                                resEl.innerText = 'Lỗi: ' + e.message;
                            }
                        }

                        setInterval(refresh, 1000);
                        window.addEventListener('resize', refresh);
                        refresh();
                    </script>
                </body>
                </html>
                """;

        sendResponse(exchange, 200, "text/html; charset=UTF-8", html);
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String contentType, String content)
            throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}

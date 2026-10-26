package vaft.dashboard.impl;

import java.io.IOException;
import java.io.InputStream;
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

import vaft.config.NodeInfo;
import vaft.core.NodeRole;
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
    private final RaftState raftState;
    private final RaftLogView logView;
    private final StateMachine stateMachine;
    private final NetworkPartitionTable partitionTable;
    private final ReplicationService replicationService;

    private HttpServer server;

    public DashboardAdminServerImpl(
            int port,
            NodeInfo self,
            RaftState raftState,
            RaftLogView logView,
            StateMachine stateMachine,
            NetworkPartitionTable partitionTable,
            ReplicationService replicationService) {
        this.port = port;
        this.self = self;
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

            // 1. Endpoint tra ve giao dien Web UI
            server.createContext("/", this::handleWebUi);

            // 2. Endpoint API lay trang thai Node
            server.createContext("/status", this::handleGetStatus);
            server.createContext("/api/status", this::handleGetStatus);

            // 3. Endpoint Chaos Testing: Cat mang & Noi mang
            server.createContext("/chaos/isolate", this::handleIsolate);
            server.createContext("/chaos/heal", this::handleHeal);

            // 4. Endpoint nhan lenh tu Client
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

            return new NodeStatusSnapshot(
                    self.getId(),
                    raftState.getRole(),
                    raftState.getCurrentTerm(),
                    raftState.getVotedFor(),
                    raftState.getLeaderId(),
                    raftState.getCommitIndex(),
                    logs,
                    fsmData,
                    isolated);
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
                    <title>VAFT Node Dashboard</title>
                    <style>
                        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
                        .container { max-width: 900px; margin: 0 auto; }
                        h1 { color: #38bdf8; display: flex; align-items: center; justify-content: space-between; }
                        .card { background: #1e293b; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.3); }
                        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 20px; }
                        .stat { background: #0f172a; padding: 15px; border-radius: 8px; text-align: center; }
                        .stat-label { font-size: 13px; color: #94a3b8; margin-bottom: 5px; }
                        .stat-value { font-size: 22px; font-weight: bold; }
                        .badge { padding: 4px 10px; border-radius: 12px; font-size: 14px; font-weight: bold; display: inline-block; }
                        .badge-LEADER { background: #22c55e; color: #000; }
                        .badge-FOLLOWER { background: #3b82f6; color: #fff; }
                        .badge-CANDIDATE { background: #f59e0b; color: #000; }
                        .badge-ISOLATED { background: #ef4444; color: #fff; }
                        .btn { padding: 10px 18px; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; transition: 0.2s; margin-right: 10px; }
                        .btn-danger { background: #ef4444; color: white; }
                        .btn-success { background: #22c55e; color: white; }
                        .btn-primary { background: #38bdf8; color: #000; }
                        .btn:hover { opacity: 0.85; }
                        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
                        th, td { border: 1px solid #334155; padding: 8px 12px; text-align: left; }
                        th { background: #0f172a; color: #38bdf8; }
                        input[type="text"] { width: 70%; padding: 10px; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: white; margin-right: 10px; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>VAFT Node Dashboard <span id="nodeBadge" class="badge">Node ...</span></h1>

                        <div class="card">
                            <div class="grid">
                                <div class="stat"><div class="stat-label">VAI TRÒ</div><div class="stat-value" id="role">-</div></div>
                                <div class="stat"><div class="stat-label">NHIỆM KỲ (TERM)</div><div class="stat-value" id="term">-</div></div>
                                <div class="stat"><div class="stat-label">LEADER HIỆN TẠI</div><div class="stat-value" id="leader">-</div></div>
                                <div class="stat"><div class="stat-label">COMMIT INDEX</div><div class="stat-value" id="commit">-</div></div>
                                <div class="stat"><div class="stat-label">MẠNG</div><div class="stat-value" id="network">-</div></div>
                            </div>

                            <div style="margin-top: 15px;">
                                <strong>Chaos Testing: </strong>
                                <button class="btn btn-danger" onclick="chaos('isolate')">Cắt mạng (Isolate)</button>
                                <button class="btn btn-success" onclick="chaos('heal')">Nối mạng (Heal)</button>
                            </div>
                        </div>

                        <div class="card">
                            <h3>Gửi lệnh Client (Key-Value)</h3>
                            <div style="display: flex;">
                                <input type="text" id="cmdInput" placeholder="VD: SET name=BKDN hoặc SET x=100">
                                <button class="btn btn-primary" onclick="sendCommand()">Gửi lệnh</button>
                            </div>
                            <div id="cmdResult" style="margin-top: 10px; color: #38bdf8; font-size: 14px;"></div>
                        </div>

                        <div class="card">
                            <h3>Raft Log Entries</h3>
                            <table>
                                <thead><tr><th>Index</th><th>Term</th><th>Command</th></tr></thead>
                                <tbody id="logTable"><tr><td colspan="3">Chưa có log</td></tr></tbody>
                            </table>
                        </div>
                    </div>

                    <script>
                        async function refresh() {
                            try {
                                const res = await fetch('/api/status');
                                const d = await res.json();
                                document.getElementById('nodeBadge').innerText = 'Node ' + d.nodeId;
                                document.getElementById('role').innerHTML = `<span class="badge badge-${d.role}">${d.role}</span>`;
                                document.getElementById('term').innerText = d.currentTerm;
                                document.getElementById('leader').innerText = d.leaderId > 0 ? 'Node ' + d.leaderId : 'Chưa rõ';
                                document.getElementById('commit').innerText = d.commitIndex;
                                document.getElementById('network').innerHTML = d.networkIsolated
                                    ? '<span class="badge badge-ISOLATED">BỊ CÔ LẬP</span>'
                                    : '<span class="badge badge-LEADER">BÌNH THƯỜNG</span>';

                                const tbody = document.getElementById('logTable');
                                if (d.log && d.log.length > 0) {
                                    tbody.innerHTML = d.log.map(e => `<tr><td>${e.index}</td><td>${e.term}</td><td>${e.command || '-'}</td></tr>`).join('');
                                }
                            } catch (e) {
                                console.error(e);
                            }
                        }

                        async function chaos(action) {
                            await fetch('/chaos/' + action, { method: 'POST' });
                            refresh();
                        }

                        async function sendCommand() {
                            const cmd = document.getElementById('cmdInput').value;
                            if (!cmd) return;
                            const resEl = document.getElementById('cmdResult');
                            resEl.innerText = 'Đang gửi...';
                            try {
                                const res = await fetch('/client/command', { method: 'POST', body: cmd });
                                const d = await res.json();
                                resEl.innerText = JSON.stringify(d);
                                refresh();
                            } catch (e) {
                                resEl.innerText = 'Lỗi: ' + e.message;
                            }
                        }

                        setInterval(refresh, 1000);
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

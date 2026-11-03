package vaft.dashboard.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import vaft.core.RaftState;
import vaft.dashboard.DashboardAdminServer;
import vaft.dashboard.JsonPacker;
import vaft.dashboard.NodeStatusSnapshot;
import vaft.replication.RaftLogManager;
import vaft.replication.ReplicationService;
import vaft.state.StateMachine;
import vaft.transport.NetworkPartitionTable;
public class DashboardAdminServerImpl implements DashboardAdminServer {

    private final int port;
    private final int selfId;
    private final RaftState raftState;
    private final RaftLogManager logManager;
    private final StateMachine stateMachine;
    private final NetworkPartitionTable partitionTable;
    private final ReplicationService replicationService;

    private HttpServer server;

    public DashboardAdminServerImpl(
            int selfId,
            int port,
            RaftState raftState,
            RaftLogManager logManager,
            StateMachine stateMachine,
            NetworkPartitionTable partitionTable,
            ReplicationService replicationService) {
        this.selfId = selfId;
        this.port = port;
        this.raftState = raftState;
        this.logManager = logManager;
        this.stateMachine = stateMachine;
        this.partitionTable = partitionTable;
        this.replicationService = replicationService;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor()); // Tận dụng Virtual Threads của Java 21

            // 1. Lấy Snapshot Node Status
            server.createContext("/api/status", this::handleGetStatus);

            // 2. Lấy toàn bộ Logs
            server.createContext("/api/logs", this::handleGetLogs);

            // 3. Chaos Test: Isolate / Heal
            server.createContext("/api/chaos/isolate", this::handleIsolate);
            server.createContext("/api/chaos/heal", this::handleHeal);

            // 4. Client Command
            server.createContext("/api/command", this::handleClientCommand);

            server.start();
            System.out.println("Dashboard Admin HTTP Server started on port " + port);
        } catch (IOException e) {
            System.err.println("Failed to start Dashboard Server: " + e.getMessage());
        }
    }

    private void handleGetStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }

        NodeStatusSnapshot snapshot = raftState.withLock(() -> new NodeStatusSnapshot(
                String.valueOf(selfId),
                raftState.getRole(),
                raftState.getCurrentTerm(),
                String.valueOf(raftState.getVotedFor()),
                raftState.getLeaderId(),
                raftState.getCommitIndex(),
                logManager.getAllEntriesSnapshot(),
                stateMachine != null ? stateMachine.getSnapshotData() : null,
                partitionTable != null && partitionTable.isBlocked(selfId, -1) // Kiểm tra xem có bị cô lập không
        ));

        sendJsonResponse(exchange, 200, JsonPacker.toJson(snapshot));
    }

    private void handleGetLogs(HttpExchange exchange) throws IOException {
        sendJsonResponse(exchange, 200, JsonPacker.toJson(logManager.getAllEntriesSnapshot()));
    }

    private void handleIsolate(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        if (partitionTable != null) {
            partitionTable.isolateNode(selfId);
        }
        sendJsonResponse(exchange, 200, "{\"status\": \"ISOLATED\", \"nodeId\": " + selfId + "}");
    }

    private void handleHeal(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        if (partitionTable != null) {
            partitionTable.healNode(selfId);
        }
        sendJsonResponse(exchange, 200, "{\"status\": \"HEALED\", \"nodeId\": " + selfId + "}");
    }

    private void handleClientCommand(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        if (partitionTable != null && partitionTable.isBlocked(selfId, -1)) {
            sendJsonResponse(exchange, 503, "{\"error\": \"Node is isolated and cannot process client commands.\"}");
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

        replicationService.submitClientCommand(body)
                .thenAccept(result -> {
                    try {
                        System.out.println("Client command result: " + result);
                        sendJsonResponse(exchange, result.accepted() ? 200 : 400, JsonPacker.toJson(result));
                    } catch (IOException ignored) {}
                });
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        // Hỗ trợ CORS nếu giao diện UI chạy trên Vite/React cổng khác (e.g., 3000, 5173)
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        sendResponse(exchange, statusCode, json);
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }
}
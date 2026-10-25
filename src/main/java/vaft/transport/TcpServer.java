package vaft.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.MessageType;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RpcEnvelope;

public class TcpServer {
    private final int selfNodeId;
    private final int port;
    private final RaftRpcHandler rpcHandler;
    private final NetworkPartitionTable partitionTable;

    private ServerSocket serverSocket;
    private final ExecutorService workerPool = Executors.newCachedThreadPool();
    private volatile boolean running = false;

    public TcpServer(int selfNodeId, int port, RaftRpcHandler rpcHandler, NetworkPartitionTable partitionTable) {
        this.selfNodeId = selfNodeId;
        this.port = port;
        this.rpcHandler = rpcHandler;
        this.partitionTable = partitionTable;
    }

    public void start() throws IOException {
        if (running) return;
        running = true;
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));

        Thread acceptor = new Thread(this::acceptLoop, "tcp-acceptor-" + selfNodeId);
        acceptor.setDaemon(false);
        acceptor.start();
    }

    private void acceptLoop() {
        while (running && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);
                workerPool.submit(() -> handleConnection(client));
            } catch (IOException e) {
                if (running) System.err.println("Accept error: " + e.getMessage());
            }
        }
    }

    private void handleConnection(Socket client) {
        try (client;
             DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(client.getOutputStream()))) {

            while (running && !client.isClosed()) {
                RpcEnvelope req = PacketCodec.readPacket(in);
                if (partitionTable != null && partitionTable.isBlocked(req.getSenderId(), selfNodeId)) {
                    break; // Giả lập đứt cáp chiều nhận
                }
                RpcEnvelope res = dispatch(req);
                PacketCodec.writePacket(out, res);
            }
        } catch (IOException ignored) {
            // Peer đóng kết nối hoặc rớt mạng
        }
    }

    private RpcEnvelope dispatch(RpcEnvelope req) {
        System.out.println("Received " + req.getType() + " from " + req.getSenderId() + " to " + req.getReceiverId());
        return switch (req.getType()) {
            case REQUEST_VOTE_REQ -> new RpcEnvelope(
                    MessageType.REQUEST_VOTE_RES, selfNodeId, req.getSenderId(),
                    rpcHandler.onRequestVote((RequestVoteRequest) req.getPayload()));
            case APPEND_ENTRIES_REQ -> new RpcEnvelope(
                    MessageType.APPEND_ENTRIES_RES, selfNodeId, req.getSenderId(),
                    rpcHandler.onAppendEntries((AppendEntriesRequest) req.getPayload()));
            default -> throw new IllegalArgumentException("Unexpected type: " + req.getType());
        };
    }

    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
        workerPool.shutdownNow();
    }
}
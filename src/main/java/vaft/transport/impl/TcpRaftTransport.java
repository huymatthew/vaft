package vaft.transport.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.MessageType;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.protocol.RpcEnvelope;
import vaft.transport.NetworkPartitionTable;
import vaft.transport.PeerConnection;
import vaft.transport.RaftRpcHandler;
import vaft.transport.RaftTransport;
import vaft.transport.TcpServer;

public class TcpRaftTransport implements RaftTransport {
    private final int selfNodeId;
    private final NetworkPartitionTable partitionTable;
    private final TcpServer tcpServer;
    private final Map<Integer, PeerConnection> peerConnections = new ConcurrentHashMap<>();

    public TcpRaftTransport(ClusterConfig config, RaftRpcHandler handler, NetworkPartitionTable partitionTable) {
        this.selfNodeId = config.getSelf().getId();
        this.partitionTable = partitionTable;
        this.tcpServer = new TcpServer(config.getSelf().getId(), config.getSelf().getPort(), handler, partitionTable);

        for (NodeInfo peer : config.getPeersList()) {
            peerConnections.put(peer.getId(), new PeerConnection(peer, config.getNetworkConfig().getConnectTimeoutMs(), config.getNetworkConfig().getRequestTimeoutMs()));
        }
        System.out.println("Connections count: " + peerConnections.size() + peerConnections.keySet());
    }
    @Override 
    public void start() throws IOException {
        tcpServer.start();
    }

    @Override 
    public void stop() {
        tcpServer.stop();
        peerConnections.values().forEach(PeerConnection::closeQuietly);
    }

    @Override
    public RequestVoteResponse sendRequestVote(int targetId, RequestVoteRequest req) {
        RpcEnvelope res = send(targetId, MessageType.REQUEST_VOTE_REQ, req);
        return (res != null) ? (RequestVoteResponse) res.getPayload() : null;
    }

    @Override
    public AppendEntriesResponse sendAppendEntries(int targetId, AppendEntriesRequest req) {
        RpcEnvelope res = send(targetId, MessageType.APPEND_ENTRIES_REQ, req);
        return (res != null) ? (AppendEntriesResponse) res.getPayload() : null;
    }

    private RpcEnvelope send(int targetId, MessageType type, Object payload) {
        if (partitionTable != null && partitionTable.isBlocked(selfNodeId, targetId)) {
            return null; // Giả lập đứt cáp chiều gửi
        }
        PeerConnection conn = peerConnections.get(targetId);
        if (conn == null) {
            System.err.println("No connection to target node: " + targetId);
            return null;
        }
        return conn.sendAndReceive(new RpcEnvelope(type, selfNodeId, targetId, payload));
    }
}
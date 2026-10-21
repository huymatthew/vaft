package vaft.transport;

import java.time.Duration;
import java.util.Map;
import vaft.config.NodeInfo;
import vaft.protocol.*;

public class TcpRaftTransport implements RaftTransport, AutoCloseable {
    private final NodeInfo self;
    private final Map<String, NodeInfo> peers;
    private final NetworkPartitionTable partitionTable;
    private final Duration connectTimeout;
    private final Duration soTimeout;
    private RaftRpcHandler handler;

    public TcpRaftTransport(NodeInfo self, Map<String, NodeInfo> peers,
                            NetworkPartitionTable partitionTable,
                            Duration connectTimeout, Duration soTimeout) {
        this.self = self;
        this.peers = peers;
        this.partitionTable = partitionTable;
        this.connectTimeout = connectTimeout;
        this.soTimeout = soTimeout;
    }

    public void setHandler(RaftRpcHandler handler) {
        this.handler = handler;
    }

    public void start() {
        throw new UnsupportedOperationException("TCP server is not implemented");
    }

    @Override
    public RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest request) {
        throw new UnsupportedOperationException("TCP client is not implemented");
    }

    @Override
    public AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest request) {
        throw new UnsupportedOperationException("TCP client is not implemented");
    }

    @Override
    public void close() {
    }
}

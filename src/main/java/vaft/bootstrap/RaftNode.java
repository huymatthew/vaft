package vaft.bootstrap;

import java.util.Objects;
import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.RaftState;
import vaft.dashboard.DashboardAdminServer;
import vaft.election.ElectionService;
import vaft.replication.ReplicationService;
import vaft.transport.RaftTransport;

public class RaftNode implements AutoCloseable {
    private final ClusterConfig config;
    private final NodeInfo self;
    private final RaftState state;
    private final RaftTransport transport;
    private final ElectionService electionService;
    private final ReplicationService replicationService;
    private final DashboardAdminServer dashboardServer;
    private boolean running;

    public RaftNode(ClusterConfig config, NodeInfo self, RaftState state,
                    RaftTransport transport, ElectionService electionService,
                    ReplicationService replicationService,
                    DashboardAdminServer dashboardServer) {
        this.config = Objects.requireNonNull(config);
        this.self = Objects.requireNonNull(self);
        this.state = Objects.requireNonNull(state);
        this.transport = Objects.requireNonNull(transport);
        this.electionService = Objects.requireNonNull(electionService);
        this.replicationService = Objects.requireNonNull(replicationService);
        this.dashboardServer = Objects.requireNonNull(dashboardServer);
    }

    public void start() {
        if (running) return;
        running = true;
        dashboardServer.start();
    }

    public void stop() {
        if (!running) return;
        running = false;
        dashboardServer.close();
        if (transport instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception exception) {
                throw new IllegalStateException("Unable to close Raft transport", exception);
            }
        }
    }

    public ClusterConfig getConfig() { return config; }
    public NodeInfo getSelf() { return self; }
    public RaftState getState() { return state; }
    public RaftTransport getTransport() { return transport; }
    public ElectionService getElectionService() { return electionService; }
    public ReplicationService getReplicationService() { return replicationService; }
    public DashboardAdminServer getDashboardServer() { return dashboardServer; }

    @Override
    public void close() { stop(); }
}

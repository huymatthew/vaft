package vaft.replication.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.NodeRole;
import vaft.core.RaftState;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.ClientCommandResult;
import vaft.replication.RaftLogManager;
import vaft.replication.ReplicationService;
import vaft.transport.RaftTransport;

public class TestReplicationService implements ReplicationService {
    private final RaftState raftState;
    private final RaftLogManager logManager;
    private final ClusterConfig clusterConfig;
    private final RaftTransport transport;
    private final Runnable electionTimerReset;
    private final ScheduledExecutorService scheduler;
    private final long heartbeatIntervalMs;
    private boolean ownsScheduler;
    private ScheduledFuture<?> heartbeatTask;


    private final Map<Integer, CompletableFuture<ClientCommandResult>> clientFutures;
    public TestReplicationService(
            RaftState raftState,
            RaftLogManager logManager,
            ClusterConfig clusterConfig,
            RaftTransport transport,
            ScheduledExecutorService scheduler,
            Runnable electionTimerReset) {

        this.raftState = raftState;
        this.logManager = logManager;
        this.clusterConfig = clusterConfig;
        this.transport = transport;
        this.scheduler = scheduler;
        this.ownsScheduler = false;
        this.electionTimerReset = electionTimerReset;
        this.heartbeatIntervalMs =
                clusterConfig.getElectionConfig().getHeartbeatIntervalMs();
        this.clientFutures = new ConcurrentHashMap<>();
    }

    /**
     * Compatibility constructor for bootstrap code that still provides
     * heartbeat start/stop callbacks. Heartbeat ownership remains here.
     */
    public TestReplicationService(
            RaftState raftState,
            RaftLogManager logManager,
            ClusterConfig clusterConfig,
            RaftTransport transport,
            Runnable ignoredHeartbeatStart,
            Runnable ignoredHeartbeatStop,
            Runnable electionTimerReset) {
        this(
                raftState,
                logManager,
                clusterConfig,
                transport,
                Executors.newSingleThreadScheduledExecutor(),
                electionTimerReset
        );
        this.ownsScheduler = true;
    }

    /**
     * Compatibility constructor for the original test bootstrap.
     * Without a transport, this constructor can only exercise timer behavior.
     */
    public TestReplicationService(
            RaftState raftState,
            RaftLogManager logManager,
            ClusterConfig clusterConfig,
            Runnable ignoredHeartbeatStart,
            Runnable ignoredHeartbeatStop) {
        this(
                raftState,
                logManager,
                clusterConfig,
                null,
                Executors.newSingleThreadScheduledExecutor(),
                () -> {
                }
        );
        this.ownsScheduler = true;
    }
    @Override
    public synchronized void onBecomeLeader() {
        initializeReplicationState();
        System.out.println("Becoming leader");

        if (heartbeatTask == null || heartbeatTask.isCancelled()) {
            heartbeatTask = scheduler.scheduleAtFixedRate(
                    this::onHeartbeatTick,
                    0,
                    heartbeatIntervalMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void initializeReplicationState() {
        raftState.withLock(() -> {
            int lastLogIndex = logManager.getLastLogIndex();

            for (NodeInfo peer : clusterConfig.getPeersList()) {
                raftState.getNextIndex().put(peer.getId(), lastLogIndex + 1);
                raftState.getMatchIndex().put(peer.getId(), 0);
            }
        });
    }

    @Override
    public synchronized void onStepDown() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    @Override
    public void onHeartbeatTick() {
        System.out.println("CALL HEARTBEAT TICK");
        if (!raftState.withLock(
                () -> raftState.getRole() == NodeRole.LEADER)) {
            return;
        }

        for (NodeInfo peer : clusterConfig.getPeersList()) {
            replicateToPeer(peer.getId());
            System.out.println("Replicating to peer: " + peer.getId());
        }
    }

    public void close() {
        onStepDown();
        if (ownsScheduler) {
            scheduler.shutdownNow();
        }
    }

    private void replicateToPeer(int peerId) {
        if (transport == null) {
            return;
        }

        final AppendEntriesRequest[] requestHolder = new AppendEntriesRequest[1];
        raftState.withLock(() -> {
            int nextIndex = raftState.getNextIndex().getOrDefault(
                    peerId, logManager.getLastLogIndex() + 1);
            raftState.getNextIndex().putIfAbsent(peerId, nextIndex);
            raftState.getMatchIndex().putIfAbsent(peerId, 0);

            int currentTerm = raftState.getCurrentTerm();
            int prevLogIndex = nextIndex - 1;
            requestHolder[0] = new AppendEntriesRequest(
                    currentTerm,
                    clusterConfig.getSelf().getId(),
                    prevLogIndex,
                    logManager.getTermAt(prevLogIndex),
                    java.util.List.of(),
                    raftState.getCommitIndex());
        });

        AppendEntriesResponse response =
                transport.sendAppendEntries(peerId, requestHolder[0]);
        if (response != null && response.getTerm() > raftState.getCurrentTerm()) {
            raftState.withLock(() -> raftState.checkTermAndStepDown(response.getTerm()));
            onStepDown();
        }
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        return raftState.withLock(() -> {
            int currentTerm = raftState.getCurrentTerm();
            if (req.getTerm() < currentTerm) {
                return AppendEntriesResponse.fail(currentTerm, -1, 0);
            }

            if (req.getTerm() > currentTerm) {
                raftState.checkTermAndStepDown(req.getTerm());
            }

            raftState.setRole(NodeRole.FOLLOWER);
            raftState.setLeaderId(req.getLeaderId());
            electionTimerReset.run();

            return AppendEntriesResponse.ok(
                    raftState.getCurrentTerm(),
                    logManager.getLastLogIndex());
        });
    }

    @Override
    public CompletableFuture<ClientCommandResult> submitClientCommand(String command) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void advanceCommitIndexIfPossible() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }

    @Override
    public void applyCommittedEntries() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }
}

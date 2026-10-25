package vaft.replication.impl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.NodeRole;
import vaft.core.RaftState;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.ClientCommandResult;
import vaft.protocol.LogEntry;
import vaft.replication.RaftLogManager;
import vaft.replication.ReplicationService;
import vaft.state.StateMachine;
import vaft.transport.impl.TcpRaftTransport;

public class ReplicationServiceImpl implements ReplicationService {
    private final RaftState raftState;
    private final RaftLogManager logManager;
    private final ClusterConfig clusterConfig;
    private final TcpRaftTransport transport;
    private final ExecutorService rpcExecutor;
    private final Runnable heartbeatTimerStart;
    private final Runnable heartbeatTimerStop;
    private final Runnable electionTimerReset;
    private final StateMachine stateMachine;

    private final Map<Integer, CompletableFuture<ClientCommandResult>> clientFutures;

    public ReplicationServiceImpl(
            RaftState raftState, 
            RaftLogManager logManager,
            ClusterConfig clusterConfig, 
            TcpRaftTransport transport,
            ExecutorService rpcExecutor, 
            StateMachine stateMachine,
            Runnable heartbeatTimerStart, 
            Runnable heartbeatTimerStop, 
            Runnable electionTimerReset) {
        
        this.raftState = raftState;
        this.logManager = logManager;
        this.clusterConfig = clusterConfig;
        this.transport = transport;
        this.rpcExecutor = rpcExecutor;
        this.stateMachine = stateMachine;
        this.heartbeatTimerStart = heartbeatTimerStart;
        this.heartbeatTimerStop = heartbeatTimerStop;
        this.electionTimerReset = electionTimerReset;
        this.clientFutures = new ConcurrentHashMap<>();
    }
    
    @Override
    public void onBecomeLeader() {
        raftState.withLock(() -> {
            int lastLogIndex = logManager.getLastLogIndex();
            
            for (NodeInfo peer : clusterConfig.getPeersList()) {
                int peerId = peer.getId();
                raftState.getNextIndex().put(peerId, lastLogIndex + 1);
                raftState.getMatchIndex().put(peerId, 0); 
            }
        });

        heartbeatTimerStart.run();
        onHeartbeatTick();
    }

    @Override
    public void onStepDown() {
        heartbeatTimerStop.run();
        
        raftState.withLock(() -> {
            for (Map.Entry<Integer, CompletableFuture<ClientCommandResult>> entry : clientFutures.entrySet()) {
                entry.getValue().complete(new ClientCommandResult(
                        false, "NOT_LEADER", raftState.getLeaderId(), entry.getKey(), -1, null
                ));
            }
            clientFutures.clear();
        });
    }

    @Override
    public void onHeartbeatTick() {
        int selfId = clusterConfig.getSelf().getId();
        for (NodeInfo peer : clusterConfig.getPeersList()) {
            if (peer.getId() != selfId) {
                replicateToPeer(peer.getId());
            }
        }
    }

    private void replicateToPeer(int peerId) {
        rpcExecutor.submit(() -> {
            final AppendEntriesRequest[] requestHolder = new AppendEntriesRequest[1];
            final int[] termHolder = new int[1];

            // BƯỚC 1: Dùng withLock thay vì acquireLock
            raftState.withLock(() -> {
                if (raftState.getRole() != NodeRole.LEADER) return;

                termHolder[0] = raftState.getCurrentTerm();
                int nextIdx = raftState.getNextIndex().getOrDefault(peerId, 1);
                int prevLogIndex = nextIdx - 1;
                int prevLogTerm = logManager.getTermAt(prevLogIndex);
                List<LogEntry> entries = logManager.getEntriesFrom(nextIdx);

                // Dùng .getId() thay vì .nodeId()
                requestHolder[0] = new AppendEntriesRequest(
                        termHolder[0], clusterConfig.getSelf().getId(),
                        prevLogIndex, prevLogTerm, entries, raftState.getCommitIndex()
                );
            });

            if (requestHolder[0] == null) return;

            AppendEntriesResponse res = transport.sendAppendEntries(peerId, requestHolder[0]);
            if (res == null) return; 

            // BƯỚC 3: Dùng withLock để cập nhật trạng thái
            raftState.withLock(() -> {
                // Sửa thành res.getTerm() thay vì res.term()
                if (raftState.checkTermAndStepDown(res.getTerm())) {
                    onStepDown();
                    return;
                }

                if (raftState.getRole() != NodeRole.LEADER || raftState.getCurrentTerm() != termHolder[0]) {
                    return;
                }

                // Sửa thành isSuccess(), getMatchIndex(), getConflictTerm()...
                if (res.isSuccess()) {
                    int currentMatch = raftState.getMatchIndex().getOrDefault(peerId, 0);
                    if (res.getMatchIndex() > currentMatch) {
                        raftState.getMatchIndex().put(peerId, res.getMatchIndex());
                        raftState.getNextIndex().put(peerId, res.getMatchIndex() + 1);
                        advanceCommitIndexIfPossible(); 
                    }
                } else {
                    int nextIdx;
                    if (res.getConflictTerm() > 0) {
                        int lastIdxOfConflictTerm = logManager.findFirstIndexOfTerm(res.getConflictTerm(), requestHolder[0].getPrevLogIndex());
                        if (lastIdxOfConflictTerm > 0) {
                            nextIdx = lastIdxOfConflictTerm + 1;
                        } else {
                            nextIdx = res.getConflictIndex();
                        }
                    } else {
                        nextIdx = res.getConflictIndex();
                    }
                    raftState.getNextIndex().put(peerId, Math.max(1, nextIdx));
                    replicateToPeer(peerId); 
                }
            });
        });
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        // Dùng mảng 1 phần tử để trả kết quả từ trong withLock ra ngoài
        final AppendEntriesResponse[] responseHolder = new AppendEntriesResponse[1];

        raftState.withLock(() -> {
            int currentTerm = raftState.getCurrentTerm();

            // Dùng getTerm() thay vì term()
            if (req.getTerm() < currentTerm) {
                // Giả định bạn đã tạo hàm tĩnh fail() và ok() trong class AppendEntriesResponse
                responseHolder[0] = AppendEntriesResponse.fail(currentTerm, -1, 0);
                return;
            }

            if (req.getTerm() > currentTerm || raftState.getRole() != NodeRole.FOLLOWER) {
                raftState.checkTermAndStepDown(req.getTerm());
            }
            raftState.setLeaderId(req.getLeaderId());
            electionTimerReset.run(); 

            // Dùng getPrevLogIndex() và getPrevLogTerm()
            if (!logManager.matchLog(req.getPrevLogIndex(), req.getPrevLogTerm())) {
                int conflictIndex = logManager.getLastLogIndex() + 1;
                int conflictTerm = 0;
                
                if (req.getPrevLogIndex() <= logManager.getLastLogIndex()) {
                    conflictTerm = logManager.getTermAt(req.getPrevLogIndex());
                    conflictIndex = req.getPrevLogIndex();
                    while (conflictIndex > 1 && logManager.getTermAt(conflictIndex - 1) == conflictTerm) {
                        conflictIndex--;
                    }
                }
                responseHolder[0] = AppendEntriesResponse.fail(currentTerm, conflictIndex, conflictTerm);
                return;
            }

            // Dùng getEntries() thay vì entries()
            logManager.appendEntriesFromLeader(req.getPrevLogIndex(), req.getEntries());

            if (req.getLeaderCommit() > raftState.getCommitIndex()) {
                raftState.setCommitIndex(Math.min(req.getLeaderCommit(), logManager.getLastLogIndex()));
                applyCommittedEntries();
            }

            responseHolder[0] = AppendEntriesResponse.ok(currentTerm, logManager.getLastLogIndex());
        });

        return responseHolder[0];
    }

    @Override
    public CompletableFuture<ClientCommandResult> submitClientCommand(String command) {
        CompletableFuture<ClientCommandResult> future = new CompletableFuture<>();
        
        raftState.withLock(() -> {
            if (raftState.getRole() != NodeRole.LEADER) {
                future.complete(new ClientCommandResult(
                        false, "NOT_LEADER", raftState.getLeaderId(), -1, -1, null
                ));
                return;
            }

            // Gọi getIndex() thay vì index()
            LogEntry newEntry = logManager.appendCommand(raftState.getCurrentTerm(), command);
            clientFutures.put(newEntry.index(), future);
            
            onHeartbeatTick(); 
        });
        
        return future;
    }

    @Override
    public void advanceCommitIndexIfPossible() {
        List<Integer> matchIndexes = new ArrayList<>(raftState.getMatchIndex().values());
        matchIndexes.add(logManager.getLastLogIndex()); 
        
        matchIndexes.sort(Collections.reverseOrder());
        
        int majorityIndex = matchIndexes.get(clusterConfig.getQuorumSize() - 1);

        if (majorityIndex > raftState.getCommitIndex() && logManager.getTermAt(majorityIndex) == raftState.getCurrentTerm()) {
            raftState.setCommitIndex(majorityIndex);
            applyCommittedEntries();
        }
    }

    @Override
    public void applyCommittedEntries() {
        while (raftState.getLastApplied() < raftState.getCommitIndex()) {
            int applyIdx = raftState.getLastApplied() + 1;
            LogEntry entry = logManager.getEntry(applyIdx);
            
            // Gọi getCommand() thay vì command()
            if (entry.command() != null) {
                stateMachine.apply(entry.command());
            }
            
            raftState.setLastApplied(applyIdx);

            CompletableFuture<ClientCommandResult> future = clientFutures.remove(applyIdx);
            if (future != null) {
                // Gọi getTerm() thay vì term()
                future.complete(new ClientCommandResult(
                        true, "COMMITTED", raftState.getLeaderId(), applyIdx, entry.term(), null
                ));
            }
        }
    }
}
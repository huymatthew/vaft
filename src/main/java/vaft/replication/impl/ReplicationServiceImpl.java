package vaft.replication.impl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
import vaft.protocol.LogEntry;
import vaft.replication.RaftLogManager;
import vaft.replication.ReplicationService;
import vaft.state.StateMachine;
import vaft.transport.RaftTransport;

public class ReplicationServiceImpl implements ReplicationService {
    private final RaftState raftState;
    private final RaftLogManager logManager;
    private final ClusterConfig clusterConfig;
    private final RaftTransport transport;
    private final ExecutorService rpcExecutor;
    private final ScheduledExecutorService scheduler;
    private final Runnable heartbeatTimerStart;
    private final Runnable heartbeatTimerStop;
    private final Runnable electionTimerReset;
    private final StateMachine stateMachine;
    private ScheduledFuture<?> heartbeatTask;
    private final Set<Integer> replicatingPeers = ConcurrentHashMap.newKeySet();    

    private final Map<Integer, CompletableFuture<ClientCommandResult>> clientFutures;
    // Chống bão task: Mỗi peer chỉ có tối đa 1 replication task in-flight tại một thời điểm
    private final Set<Integer> inflightReplications = ConcurrentHashMap.newKeySet();

    public ReplicationServiceImpl(
            RaftState raftState, 
            RaftLogManager logManager, 
            ClusterConfig clusterConfig, 
            RaftTransport transport,
            ExecutorService rpcExecutor, 
            ScheduledExecutorService scheduler,
            StateMachine stateMachine,
            Runnable heartbeatTimerStart, 
            Runnable heartbeatTimerStop, 
            Runnable electionTimerReset) {
        
        this.raftState = raftState;
        this.logManager = logManager;
        this.clusterConfig = clusterConfig;
        this.transport = transport;
        this.rpcExecutor = rpcExecutor;
        this.scheduler = scheduler;
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

        if (heartbeatTimerStart != null) {
            heartbeatTimerStart.run();
        }
        if (heartbeatTask == null || heartbeatTask.isCancelled()) {
            heartbeatTask = scheduler.scheduleAtFixedRate(
                    this::onHeartbeatTick,
                    0,
                    clusterConfig.getElectionConfig().getHeartbeatIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public void onStepDown() {
        if (heartbeatTimerStop != null) {
            heartbeatTimerStop.run();
        }
        inflightReplications.clear();
        
        raftState.withLock(() -> {
            for (Map.Entry<Integer, CompletableFuture<ClientCommandResult>> entry : clientFutures.entrySet()) {
                entry.getValue().complete(new ClientCommandResult(
                        false, "NOT_LEADER", raftState.getLeaderId(), entry.getKey(), -1, null
                ));
            }
            clientFutures.clear();
        });

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
        
        int selfId = clusterConfig.getSelf().getId();
        for (NodeInfo peer : clusterConfig.getPeersList()) {
            if (peer.getId() != selfId) {
                replicateToPeer(peer.getId());
            }
        }
    }

    private void replicateToPeer(int peerId) {
        // Chống bão task: Mỗi peer chỉ có tối đa 1 replication task đang chạy
        if (!inflightReplications.add(peerId)) {
            return;
        }

        rpcExecutor.submit(() -> {
            try {
                // Vòng lặp đồng bộ tuần tự thay cho đệ quy: tránh cạn kiệt thread pool và tràn bộ nhớ
                while (true) {
                    final AppendEntriesRequest[] requestHolder = new AppendEntriesRequest[1];
                    final int[] termHolder = new int[1];

                    boolean canReplicate = raftState.withLock(() -> {
                        if (raftState.getRole() != NodeRole.LEADER) return false;

                        termHolder[0] = raftState.getCurrentTerm();
                        int nextIdx = raftState.getNextIndex().getOrDefault(peerId, 1);
                        int prevLogIndex = nextIdx - 1;
                        int prevLogTerm = logManager.getTermAt(prevLogIndex);
                        List<LogEntry> entries = logManager.getEntriesFrom(nextIdx);

                        requestHolder[0] = new AppendEntriesRequest(
                                termHolder[0], clusterConfig.getSelf().getId(),
                                prevLogIndex, prevLogTerm, entries, raftState.getCommitIndex()
                        );
                        return true;
                    });

                    if (!canReplicate || requestHolder[0] == null) {
                        break;
                    }

                    // Gửi RPC mạng ngoài lock
                    AppendEntriesResponse res;
                    try {
                        res = transport.sendAppendEntries(peerId, requestHolder[0]);
                    } catch (Exception e) {
                        // Lỗi kết nối hoặc đứt mạng: Dừng lại, đợi nhịp heartbeat tiếp theo thử lại
                        break;
                    }
                    if (res == null) {
                        // Lỗi kết nối hoặc timeout: Dừng lại, đợi nhịp heartbeat tiếp theo thử lại
                        break;
                    }

                    boolean shouldContinue = raftState.withLock(() -> {
                        if (raftState.checkTermAndStepDown(res.getTerm())) {
                            onStepDown();
                            return false;
                        }

                        if (raftState.getRole() != NodeRole.LEADER || raftState.getCurrentTerm() != termHolder[0]) {
                            return false;
                        }

                        if (res.isSuccess()) {
                            int currentMatch = raftState.getMatchIndex().getOrDefault(peerId, 0);
                            if (res.getMatchIndex() > currentMatch) {
                                raftState.getMatchIndex().put(peerId, res.getMatchIndex());
                                raftState.getNextIndex().put(peerId, res.getMatchIndex() + 1);
                                advanceCommitIndexIfPossible();

                                // Nếu Follower vẫn còn thiếu log mới so với Leader, tiếp tục vòng lặp gửi tiếp ngay
                                int nextIdx = raftState.getNextIndex().getOrDefault(peerId, 1);
                                return nextIdx <= logManager.getLastLogIndex();
                            }
                            // Nếu matchIndex không tăng (stale response): không lặp lại
                            return false;
                        } else {
                            // Xử lý xung đột log: lùi nextIndex an toàn (chống stale response)
                            int currentNext = raftState.getNextIndex().getOrDefault(peerId, 1);
                            if (requestHolder[0].getPrevLogIndex() == currentNext - 1) {
                                int nextIdx;
                                if (res.getConflictTerm() > 0) {
                                    int lastIdxOfConflictTerm = logManager.findLastIndexOfTerm(res.getConflictTerm(), requestHolder[0].getPrevLogIndex());
                                    if (lastIdxOfConflictTerm > 0) {
                                        nextIdx = lastIdxOfConflictTerm + 1;
                                    } else {
                                        nextIdx = res.getConflictIndex();
                                    }
                                } else {
                                    nextIdx = res.getConflictIndex();
                                }

                                // Đảm bảo nextIndex luôn giảm ít nhất 1 đơn vị và >= 1
                                int updatedNext = Math.max(1, Math.min(currentNext - 1, nextIdx));
                                raftState.getNextIndex().put(peerId, updatedNext);
                                return true; // Tiếp tục vòng lặp để gửi lại với nextIndex đã lùi
                            }
                            return false;
                        }
                    });

                    if (!shouldContinue) {
                        break;
                    }
                }
            } finally {
                inflightReplications.remove(peerId);
            }
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

            if (req.getTerm() > currentTerm) {
                raftState.checkTermAndStepDown(req.getTerm());
                currentTerm = raftState.getCurrentTerm();
            }

            // Theo Raft §5.2: Nếu Candidate hoặc Leader nhận AppendEntries hợp lệ (term >= currentTerm)
            // thì bắt buộc phải chuyển vai trò về FOLLOWER
            if (raftState.getRole() != NodeRole.FOLLOWER) {
                if (raftState.getRole() == NodeRole.LEADER) {
                    onStepDown();
                }
                raftState.setRole(NodeRole.FOLLOWER);
            }

            raftState.setLeaderId(req.getLeaderId());
            if (electionTimerReset != null) {
                electionTimerReset.run();
            } 

            // Dùng getPrevLogIndex() và getPrevLogTerm()
            if (!logManager.matchLog(req.getPrevLogIndex(), req.getPrevLogTerm())) {
                int conflictIndex = logManager.getLastLogIndex() + 1;
                int conflictTerm = 0;
                
                if (req.getPrevLogIndex() <= logManager.getLastLogIndex() && req.getPrevLogIndex() > 0) {
                    conflictTerm = logManager.getTermAt(req.getPrevLogIndex());
                    int firstIdx = logManager.findFirstIndexOfTerm(conflictTerm, req.getPrevLogIndex());
                    conflictIndex = firstIdx > 0 ? firstIdx : req.getPrevLogIndex();
                }
                responseHolder[0] = AppendEntriesResponse.fail(currentTerm, conflictIndex, conflictTerm);
                return;
            }

            // Dùng getEntries() thay vì entries(), truyền commitIndex để bảo vệ log đã commit không bị truncate
            logManager.appendEntriesFromLeader(req.getPrevLogIndex(), req.getEntries(), raftState.getCommitIndex());

            // Chỉ số của entry mới cuối cùng được xác thực khớp trong bản tin này
            int lastNewEntryIndex = req.getPrevLogIndex() + (req.getEntries() != null ? req.getEntries().size() : 0);

            // Raft Rule (Figure 2): If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (req.getLeaderCommit() > raftState.getCommitIndex()) {
                int newCommitIndex = Math.min(req.getLeaderCommit(), lastNewEntryIndex);
                // Đảm bảo commitIndex chỉ tăng đơn điệu, không bao giờ bị lùi
                if (newCommitIndex > raftState.getCommitIndex()) {
                    raftState.setCommitIndex(newCommitIndex);
                    applyCommittedEntries();
                }
            }

            responseHolder[0] = AppendEntriesResponse.ok(currentTerm, lastNewEntryIndex);
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
            
            // Tự động kiểm tra commit ngay (đặc biệt cần thiết cho cụm 1 node có quorum = 1)
            advanceCommitIndexIfPossible();

            onHeartbeatTick(); 
        });
        
        return future;
    }

    @Override
    public void advanceCommitIndexIfPossible() {
        int quorumSize = clusterConfig.getQuorumSize();
        if (quorumSize <= 0) {
            return;
        }

        List<Integer> matchIndexes = new ArrayList<>();
        // 1. Thêm matchIndex của Leader (chính là lastLogIndex)
        matchIndexes.add(logManager.getLastLogIndex());

        // 2. Thêm matchIndex của các peer (mặc định 0 nếu peer chưa kết nối hoặc chưa phản hồi)
        int selfId = clusterConfig.getSelf() != null ? clusterConfig.getSelf().getId() : -1;
        if (clusterConfig.getNodes() != null) {
            for (NodeInfo node : clusterConfig.getNodes()) {
                if (node.getId() != selfId) {
                    matchIndexes.add(raftState.getMatchIndex().getOrDefault(node.getId(), 0));
                }
            }
        } else {
            matchIndexes.addAll(raftState.getMatchIndex().values());
        }

        // 3. Phòng thủ: Nếu số lượng node trong cluster nhỏ hơn Quorum thì không thể đạt đa số
        if (matchIndexes.size() < quorumSize) {
            return;
        }

        matchIndexes.sort(Collections.reverseOrder());

        int majorityIndex = matchIndexes.get(quorumSize - 1);

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
            
            Throwable applyError = null;
            if (entry != null && entry.command() != null) {
                try {
                    stateMachine.apply(entry.command());
                } catch (Throwable t) {
                    applyError = t;
                    System.err.println("Lỗi thực thi lệnh tại State Machine (index " + applyIdx + "): " + t.getMessage());
                }
            }
            
            // Bắt buộc luôn tăng lastApplied để tránh kẹt vĩnh viễn luồng commit của Raft
            raftState.setLastApplied(applyIdx);

            CompletableFuture<ClientCommandResult> future = clientFutures.remove(applyIdx);
            if (future != null) {
                int term = (entry != null) ? entry.term() : -1;
                if (applyError == null) {
                    future.complete(new ClientCommandResult(
                            true, "COMMITTED", raftState.getLeaderId(), applyIdx, term, null
                    ));
                } else {
                    future.complete(new ClientCommandResult(
                            false, "EXECUTION_ERROR: " + applyError.getMessage(), raftState.getLeaderId(), applyIdx, term, null
                    ));
                }
            }
        }
    }
}
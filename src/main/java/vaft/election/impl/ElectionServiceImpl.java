package vaft.election.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import vaft.concurrent.ResettableTimer;
import vaft.config.ClusterConfig;
import vaft.config.ElectionConfig;
import vaft.config.NodeInfo;
import vaft.core.NodeRole;
import vaft.core.RaftState;
import vaft.election.ElectionService;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.replication.RaftLogView;
import vaft.replication.ReplicationService;
import vaft.transport.RaftTransport;

public class ElectionServiceImpl implements ElectionService {

    private final RaftState raftState;
    private final RaftLogView logView;
    private final ClusterConfig clusterConfig;
    private final RaftTransport transport;
    private final ExecutorService rpcExecutor;
    private final ResettableTimer electionTimer;
    private final ReplicationService replicationService;

    public ElectionServiceImpl(
        RaftState raftState,
        RaftLogView logView,
        ClusterConfig clusterConfig,
        RaftTransport transport,
        ExecutorService rpcExecutor,
        ScheduledExecutorService scheduler,
        ReplicationService replicationService) {

        this.raftState = raftState;
        this.logView = logView;
        this.clusterConfig = clusterConfig;
        this.transport = transport;
        this.rpcExecutor = rpcExecutor;
        this.replicationService = replicationService;

        ElectionConfig electionConfig = clusterConfig.getElectionConfig();

        this.electionTimer = new ResettableTimer(
                scheduler,
                electionConfig.getMinTimeoutMs(),
                electionConfig.getMaxTimeoutMs(),
                this::onElectionTimeout
        );
        this.electionTimer.reset();
    }

    @Override
    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        return raftState.withLock(() -> {
            int currentTerm = raftState.getCurrentTerm();

            // Term của candidate cũ hơn thì từ chối ngay
            if (req.getTerm() < currentTerm) {
                return new RequestVoteResponse(currentTerm, false);
            }

            // Gặp term lớn hơn thì cập nhật và lùi về Follower
            if (req.getTerm() > currentTerm) {
                raftState.checkTermAndStepDown(req.getTerm());
                currentTerm = raftState.getCurrentTerm();
            }

            // Chỉ vote nếu chưa bỏ phiếu cho ai khác trong term này
            String votedFor = raftState.getVotedFor();
            String candidateIdStr = String.valueOf(req.getCandidateId());
            boolean canVote = (votedFor == null || votedFor.equals(candidateIdStr));

            // Kiểm tra log của ứng viên có đủ mới hay không (Raft Figure 2)
            boolean logOk = logView.isCandidateLogUpToDate(
                    req.getLastLogIndex(),
                    req.getLastLogTerm());

            // Đồng ý bỏ phiếu và reset lại timer của node
            if (canVote && logOk) {
                raftState.setVotedFor(candidateIdStr);
                electionTimer.reset();
                return new RequestVoteResponse(currentTerm, true);
            }

            return new RequestVoteResponse(currentTerm, false);
        });
    }

    @Override
    public void onElectionTimeout() {
        final RequestVoteRequest[] requestHolder = new RequestVoteRequest[1];
        final int[] termHolder = new int[1];

        // Chuẩn bị thông tin tranh cử trong khóa
        raftState.withLock(() -> {
            if (raftState.getRole() == NodeRole.LEADER) {
                return;
            }

            // Chuyển sang Candidate, tăng term và tự bầu cho chính mình
            raftState.becomeCandidate();
            int newTerm = raftState.getCurrentTerm() + 1;
            raftState.setCurrentTerm(newTerm);
            termHolder[0] = newTerm;

            int selfId = clusterConfig.getSelf().getId();
            raftState.setVotedFor(String.valueOf(selfId));
            electionTimer.reset();

            int lastLogIndex = logView.getLastLogIndex();
            int lastLogTerm = logView.getLastLogTerm();

            requestHolder[0] = new RequestVoteRequest(newTerm, selfId, lastLogIndex, lastLogTerm);
            System.out.println("Node " + selfId + " start election term " + newTerm);
        });

        if (requestHolder[0] == null) {
            return;
        }

        int selfId = clusterConfig.getSelf().getId();
        int quorumSize = clusterConfig.getQuorumSize();
        int electionTerm = termHolder[0];

        // Khởi tạo 1 phiếu từ chính mình
        AtomicInteger votes = new AtomicInteger(1);
        AtomicBoolean won = new AtomicBoolean(false);

        if (votes.get() >= quorumSize) {
            winElection();
            return;
        }

        // Gửi RequestVote song song tới các peer ngoài khóa withLock
        for (NodeInfo peer : clusterConfig.getPeersList()) {
            if (peer.getId() == selfId) {
                continue;
            }

            rpcExecutor.submit(() -> {
                RequestVoteResponse res = transport.sendRequestVote(peer.getId(), requestHolder[0]);
                if (res == null) {
                    return;
                }

                raftState.withLock(() -> {
                    // Nếu gặp term cao hơn từ phản hồi -> step down
                    if (res.getTerm() > electionTerm) {
                        raftState.checkTermAndStepDown(res.getTerm());
                        return;
                    }

                    if (raftState.getRole() != NodeRole.CANDIDATE || raftState.getCurrentTerm() != electionTerm) {
                        return;
                    }

                    // Đếm phiếu phản hồi và kiểm tra Quorum
                    if (res.isVoteGranted()) {
                        int count = votes.incrementAndGet();
                        System.out.println("Node " + selfId + " got vote from " + peer.getId() + " (" + count + "/" + quorumSize + ")");

                        if (count >= quorumSize && won.compareAndSet(false, true)) {
                            winElection();
                        }
                    }
                });
            });
        }
    }

    private void winElection() {
        raftState.becomeLeader();
        int selfId = clusterConfig.getSelf().getId();
        int currentTerm = raftState.getCurrentTerm();
        System.out.println(">>> Node " + selfId + " became LEADER for term " + currentTerm + " <<<");

        // Đánh thức replication service gửi heartbeat đầu tiên
        if (replicationService != null) {
            replicationService.onBecomeLeader();
        }
    }
}

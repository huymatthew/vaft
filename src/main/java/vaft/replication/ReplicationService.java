package vaft.replication;

import java.util.concurrent.CompletableFuture;

import vaft.protocol.*;

public interface ReplicationService {
    /**
     * Được gọi ngay khi Node vừa đắc cử thành LEADER (bên trong RaftState.becomeLeader()).
     * 1. Khởi tạo cho mọi peer trong cụm:
     *    - nextIndex.put(peerId, logManager.getLastLogIndex() + 1)
     *    - matchIndex.put(peerId, 0)
     * 2. Bật heartbeatTimer và phát ngay 1 đợt Heartbeat đầu tiên để khẳng định quyền Leader.
     */
    void onBecomeLeader();

    /**
     * Được gọi khi Node bước xuống làm FOLLOWER hoặc CANDIDATE.
     * Nhiệm vụ: Dừng heartbeatTimer và hủy (complete với trạng thái NOT_LEADER)
     * mọi CompletableFuture của Client đang treo chờ commit trên node này.
     */
    void onStepDown();

    /**
     * Được gọi định kỳ mỗi 50ms bởi heartbeatTimer khi node đang là LEADER
     * (hoặc được gọi tức thời ngay sau khi Leader nhận lệnh mới từ Client).
     * Nhiệm vụ:
     * - Với mỗi peer trong ClusterConfig.getPeers(), đóng gói AppendEntriesRequest
     *   dựa theo nextIndex[peerId] (nếu nextIndex <= lastLogIndex thì gửi kèm entries,
     *   ngược lại gửi entries rỗng làm Heartbeat).
     * - Đẩy việc gửi TCP sang RpcExecutor (NGOÀI khóa withLock) để gửi song song tới 4 node.
     */
    void onHeartbeatTick();

    /**
     * Được gọi tự động bởi TcpRaftTransport khi nhận được gói APPEND_ENTRIES_REQ.
     * Nhiệm vụ (thực hiện trong RaftState.withLock):
     * 1. Nếu req.term() < currentTerm -> trả về AppendEntriesResponse.fail(currentTerm, ...).
     * 2. Nếu req.term() >= currentTerm:
     *    - Gọi raftState.checkTermAndStepDown(req.term()) hoặc chuyển sang FOLLOWER,
     *      ghi nhận leaderId = req.leaderId().
     *    - Gọi electionTimer.reset() vì đã nhận tín hiệu từ Leader hợp lệ!
     * 3. Kiểm tra logManager.matchLog(req.prevLogIndex(), req.prevLogTerm()):
     *    - Nếu false: tính toán conflictIndex, conflictTerm và trả về fail.
     * 4. Nếu true:
     *    - Gọi logManager.appendEntriesFromLeader(req.prevLogIndex(), req.entries()).
     *    - Nếu req.leaderCommit() > commitIndex:
     *      cập nhật commitIndex = Math.min(req.leaderCommit(), logManager.getLastLogIndex()).
     *    - Gọi applyCommittedEntries() để đẩy các lệnh mới commit vào StateMachine.
     *    - Trả về AppendEntriesResponse.ok(currentTerm, logManager.getLastLogIndex()).
     */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req);

    /**
     * Được gọi từ DashboardAdminServer (POST /client/command) khi người dùng gửi lệnh (VD: "SET x=10").
     * Nhiệm vụ:
     * 1. Nếu role != LEADER: trả về ngay CompletableFuture.completedFuture(
     *    new ClientCommandResult(false, "NOT_LEADER", raftState.getLeaderId(), -1, -1, null)).
     * 2. Nếu là LEADER:
     *    - Gọi logManager.appendCommand(currentTerm, command) để ghi vào log cục bộ.
     *    - Lưu một CompletableFuture<ClientCommandResult> vào Map<Integer, CompletableFuture> theo logIndex.
     *    - Kích hoạt ngay onHeartbeatTick() để nhân bản tới 4 Follower mà không cần chờ hết 50ms.
     *    - Khi đa số (Leader + ít nhất 2 Follower) xác nhận matchIndex >= logIndex
     *      và entry được apply vào StateMachine, hoàn tất (complete) Future này.
     */
    CompletableFuture<ClientCommandResult> submitClientCommand(String command);

    /**
     * Hàm nội bộ kiểm tra và nâng commitIndex trên Leader sau mỗi lần nhận AppendEntriesResponse thành công:
     * Tìm số N lớn nhất sao cho:
     * - N > commitIndex
     * - Số node có matchIndex[peer] >= N (cộng thêm chính Leader) >= ClusterConfig.getQuorumSize() (tức >= 3)
     * - VÀ BẮT BUỘC: logManager.getTermAt(N) == raftState.getCurrentTerm() (Luật Figure 8).
     * Nếu thỏa mãn, nâng commitIndex = N và gọi applyCommittedEntries().
     */
    void advanceCommitIndexIfPossible();

    /**
     * Đẩy tuần tự mọi LogEntry trong khoảng (lastApplied < index <= commitIndex)
     * vào StateMachine.apply(command), tăng lastApplied và đánh thức các Client Future tương ứng.
     */
    void applyCommittedEntries();
}

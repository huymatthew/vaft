package vaft.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import vaft.protocol.LogEntry;

public class RaftState {
    private int currentTerm; //Term hiện tại của Node, được tăng lên khi Node trở thành Candidate hoặc nhận được Term mới từ Leader khác.
    private String votedFor; //ID của Node mà Node này đã bỏ phiếu trong Term hiện tại. Nếu chưa bỏ phiếu, giá trị này là null.
    private NodeRole role = NodeRole.FOLLOWER; // Vai trò hiện tại của Node: FOLLOWER, CANDIDATE, hoặc LEADER.
    private int leaderId; //ID của Node mà Node này nhận diện là Leader hiện tại. Nếu chưa biết, giá trị này là null.
    private int commitIndex; // Chỉ số của log entry đã được commit (được áp dụng cho state machine). Node sẽ áp dụng các log entry từ lastApplied + 1 đến commitIndex.
    private int lastApplied; // Chỉ số của log entry đã được áp dụng cho state machine.
    private final Map<Integer, Integer> nextIndex = new HashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();
    private final List<LogEntry> log = new java.util.ArrayList<>();// Danh sách log entries, mỗi entry chứa thông tin về command và term. Chỉ số của log entry bắt đầu từ 1 (index 0 là giả định).
    private final ReentrantLock lock = new ReentrantLock();//

    public <T> T withLock(java.util.function.Supplier<T> action) {
        lock.lock();
        try { return action.get(); } finally { lock.unlock(); }
    }

    public void withLock(Runnable action) {
        lock.lock();
        try { action.run(); } finally { lock.unlock(); }
    }

    /**
     * Kiểm tra Term đến từ RPC. Nếu lớn hơn Term hiện tại thì cập nhật và lùi về làm Follower.
     * @return true nếu node thực sự đã step down.
     */
    public boolean checkTermAndStepDown(int rpcTerm) {
        if (rpcTerm > this.currentTerm) {
            this.currentTerm = rpcTerm;
            this.votedFor = null;
            this.role = NodeRole.FOLLOWER;
            // Gọi hàm lưu xuống đĩa (Persistence) ở đây nếu có
            return true;
        }
        return false;
    }


    public int getCurrentTerm() { return currentTerm; }
    public String getVotedFor() { return votedFor; }
    public NodeRole getRole() { return role; }
    public int getLeaderId() { return leaderId; }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public List<LogEntry> getLog() { return List.copyOf(log); }
    public Map<Integer, Integer> getNextIndex() { return Map.copyOf(nextIndex); } //?????
    public Map<Integer, Integer> getMatchIndex() { return Map.copyOf(matchIndex); }

    public void setCurrentTerm(int currentTerm) { this.currentTerm = currentTerm; }
    public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
    public void setRole(NodeRole role) { this.role = role; }
    public void setLeaderId(int leaderId) { this.leaderId = leaderId; }
    public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
    public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }
    public int getLastLogIndex() { return log.isEmpty() ? 0 : log.get(log.size() - 1).index(); }
    public int getLastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).term(); }

    public void becomeCandidate() { role = NodeRole.CANDIDATE; }
    public void becomeLeader() { role = NodeRole.LEADER; }
    public void becomeFollower(int term, int leaderId) {
        currentTerm = term;
        this.leaderId = leaderId;
        role = NodeRole.FOLLOWER;
    }
}

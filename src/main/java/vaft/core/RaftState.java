package vaft.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import vaft.protocol.LogEntry;
import vaft.storage.PersistenceStorage;

public class RaftState {
    private int currentTerm = 0; // Term hiện tại của Node
    private int votedFor = -1;   // ID của Node mà Node này đã bỏ phiếu trong Term hiện tại (-1 nếu chưa bầu)
    private NodeRole role = NodeRole.FOLLOWER;
    private int leaderId = -1;
    private int commitIndex = 0;
    private int lastApplied = 0;
    private int lastSnapshotIndex = 0;
    private final Map<Integer, Integer> nextIndex = new HashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();
    private final List<LogEntry> log = new java.util.ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private PersistenceStorage persistenceStorage;

    public RaftState() {
    }

    public RaftState(PersistenceStorage persistenceStorage) {
        this.persistenceStorage = persistenceStorage;
    }

    public void setPersistenceStorage(PersistenceStorage persistenceStorage) {
        this.persistenceStorage = persistenceStorage;
    }

    public PersistenceStorage getPersistenceStorage() {
        return persistenceStorage;
    }

    public int getLastSnapshotIndex() {
        return lastSnapshotIndex;
    }

    public void setLastSnapshotIndex(int lastSnapshotIndex) {
        this.lastSnapshotIndex = lastSnapshotIndex;
    }

    public void restoreMeta(int currentTerm, int votedFor) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    private void persistMeta() {
        if (persistenceStorage != null) {
            try {
                persistenceStorage.saveMeta(this.currentTerm, this.votedFor);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to save Raft metadata (currentTerm, votedFor)", e);
            }
        }
    }

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
            this.votedFor = -1;
            this.role = NodeRole.FOLLOWER;
            persistMeta();
            return true;
        }
        return false;
    }

    public int getCurrentTerm() { return currentTerm; }
    public int getVotedFor() { return votedFor; }
    public NodeRole getRole() { return role; }
    public int getLeaderId() { return leaderId; }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public List<LogEntry> getLog() { return List.copyOf(log); }
    public Map<Integer, Integer> getNextIndex() { return nextIndex; }
    public Map<Integer, Integer> getMatchIndex() { return matchIndex; }

    public void setCurrentTerm(int currentTerm) {
        if (this.currentTerm != currentTerm) {
            this.currentTerm = currentTerm;
            persistMeta();
        }
    }

    public void setVotedFor(int votedFor) {
        if (this.votedFor != votedFor) {
            this.votedFor = votedFor;
            persistMeta();
        }
    }

    public void setRole(NodeRole role) { this.role = role; }
    public void setLeaderId(int leaderId) { this.leaderId = leaderId; }
    public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
    public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }
    public int getLastLogIndex() { return log.isEmpty() ? 0 : log.get(log.size() - 1).index(); }
    public int getLastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).term(); }

    public void becomeCandidate() { role = NodeRole.CANDIDATE; }
    public void becomeLeader() { role = NodeRole.LEADER; }
    public void becomeFollower(int term, int leaderId) {
        boolean termChanged = (this.currentTerm != term);
        this.currentTerm = term;
        this.leaderId = leaderId;
        this.role = NodeRole.FOLLOWER;
        if (termChanged) {
            persistMeta();
        }
    }
}

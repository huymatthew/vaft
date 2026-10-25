package vaft.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import vaft.protocol.LogEntry;

public class RaftState {
    private int currentTerm;
    private String votedFor;
    private NodeRole role = NodeRole.FOLLOWER;
    private String leaderId;
    private int commitIndex;
    private int lastApplied;
    private final Map<String, Integer> nextIndex = new HashMap<>();
    private final Map<String, Integer> matchIndex = new HashMap<>();
    private final List<LogEntry> log = new java.util.ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();

    public <T> T withLock(java.util.function.Supplier<T> action) {
        lock.lock();
        try { return action.get(); } finally { lock.unlock(); }
    }

    public void withLock(Runnable action) {
        lock.lock();
        try { action.run(); } finally { lock.unlock(); }
    }

    public int getCurrentTerm() { return currentTerm; }
    public String getVotedFor() { return votedFor; }
    public NodeRole getRole() { return role; }
    public String getLeaderId() { return leaderId; }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public List<LogEntry> getLog() { return List.copyOf(log); }
    public Map<String, Integer> getNextIndex() { return Map.copyOf(nextIndex); }
    public Map<String, Integer> getMatchIndex() { return Map.copyOf(matchIndex); }

    public void setCurrentTerm(int currentTerm) { this.currentTerm = currentTerm; }
    public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
    public void setRole(NodeRole role) { this.role = role; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
    public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }
    public int getLastLogIndex() { return log.isEmpty() ? 0 : log.get(log.size() - 1).index(); }
    public int getLastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).term(); }

    public void becomeCandidate() { role = NodeRole.CANDIDATE; }
    public void becomeLeader() { role = NodeRole.LEADER; }
    public void becomeFollower(int term, String leaderId) {
        currentTerm = term;
        this.leaderId = leaderId;
        role = NodeRole.FOLLOWER;
    }
}

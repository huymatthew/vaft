package vaft.replication.impl;
import java.util.List;

import vaft.protocol.LogEntry;
import vaft.replication.RaftLogView;
public class TestLog implements RaftLogView {
    @Override
    public int getLastLogIndex() {
        return 0;
    }

    @Override
    public int getLastLogTerm() {
        return 0;
    }

    @Override
    public int getTermAt(int index) {
        return 0;
    }

    @Override
    public LogEntry getEntry(int index) {
        return null;
    }

    @Override
    public boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        return true; // Luon luon chap nhan
    }
 
    @Override 
    public List<LogEntry> getAllEntriesSnapshot() {
        return null;
    }
}

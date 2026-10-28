package vaft.replication.impl;

import java.util.Collections;
import java.util.List;

import vaft.protocol.LogEntry;
import vaft.replication.RaftLogManager;

public class TestLogManager implements RaftLogManager {
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
    public List<LogEntry> getAllEntriesSnapshot() {
        return Collections.emptyList();
    }

    @Override
    public boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        return false;
    }

    @Override
    public LogEntry appendCommand(int currentTerm, String command) {
        return null;
    }

    @Override
    public List<LogEntry> getEntriesFrom(int startIndex) {
        return Collections.emptyList();
    }

    @Override
    public boolean matchLog(int prevLogIndex, int prevLogTerm) {
        return prevLogIndex == 0;
    }

    @Override
    public void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries) {
        // No log state is maintained by this test implementation.
    }

    @Override
    public int findFirstIndexOfTerm(int conflictTerm, int maxSearchIndex) {
        return 0;
    }
}

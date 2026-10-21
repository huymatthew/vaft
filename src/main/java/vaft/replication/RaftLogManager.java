package vaft.replication;

import java.util.List;
import vaft.protocol.LogEntry;

public interface RaftLogManager {
    List<LogEntry> getEntries();
    int getLastLogIndex();
    int getLastLogTerm();
}

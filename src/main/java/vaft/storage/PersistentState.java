package vaft.storage;

import java.util.List;
import vaft.protocol.LogEntry;

public record PersistentState(int currentTerm, String votedFor, List<LogEntry> log) {}

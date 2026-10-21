package vaft.dashboard;

import java.util.List;
import java.util.Map;
import vaft.core.NodeRole;
import vaft.protocol.LogEntry;

public record NodeStatusSnapshot(String nodeId, NodeRole role, int currentTerm,
        String votedFor, String leaderId, int commitIndex, List<LogEntry> log,
        Map<String, String> stateMachine, boolean networkIsolated) {}

package vaft.transport;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkPartitionTable {
    private final Set<String> blockedLinks = ConcurrentHashMap.newKeySet();
    public void isolateNode(String nodeId) { blockedLinks.add(nodeId + "->*"); blockedLinks.add("*->" + nodeId); }
    public void healNode(String nodeId) { blockedLinks.removeIf(link -> link.equals(nodeId + "->*") || link.equals("*->" + nodeId)); }
    public void blockLink(String from, String to) { blockedLinks.add(from + "->" + to); }
    public boolean isBlocked(String from, String to) {
        return blockedLinks.contains(from + "->" + to) || blockedLinks.contains(from + "->*") || blockedLinks.contains("*->" + to);
    }
}

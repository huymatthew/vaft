package vaft.transport;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkPartitionTable {
    private final Set<String> blockedLinks = ConcurrentHashMap.newKeySet();
    public void isolateNode(int nodeId) { blockedLinks.add(nodeId + "->*"); blockedLinks.add("*->" + nodeId); }
    public void healNode(int nodeId) { blockedLinks.removeIf(link -> link.equals(nodeId + "->*") || link.equals("*->" + nodeId)); }
    public void blockLink(int from, int to) { blockedLinks.add(from + "->" + to); }
    public boolean isBlocked(int from, int to) {
        return blockedLinks.contains(from + "->" + to) || blockedLinks.contains(from + "->*") || blockedLinks.contains("*->" + to);
    }
}
    
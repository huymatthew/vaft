package vaft.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {
    private final Map<String, String> data = new ConcurrentHashMap<>();
    public void apply(String command) { throw new UnsupportedOperationException("State-machine execution is not implemented"); }
    public Map<String, String> getSnapshotData() { return Map.copyOf(data); }
}

package vaft.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {
    private final Map<String, String> data = new ConcurrentHashMap<>();
    public void apply(String command) { 
        // Command là "SET key=value or SET key = value"
        String[] parts = command.split(" ", 2); //!!! Tạm thời chỉ mới SET
        if (parts.length == 2 && parts[0].equals("SET")) {
            String[] keyValue = parts[1].split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                data.put(key, value);
            }
        }
    }
    public Map<String, String> getSnapshotData() { return Map.copyOf(data); }
}

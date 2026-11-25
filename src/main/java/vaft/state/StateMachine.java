package vaft.state;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Replicated State Machine (Key-Value Store in-memory).
 * Đảm bảo tính tất định (Deterministic): Cùng chuỗi lệnh thì mọi Node đều có
 * cùng trạng thái dữ liệu.
 */
public class StateMachine {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    /**
     * Thực thi lệnh từ Raft Log và trả về chuỗi kết quả (dùng cho client).
     * Tương thích với interface cũ: vẫn nhận String rawCommand.
     */
    public synchronized String apply(String rawCommand) {
        Command cmd = Command.parse(rawCommand);
        CommandResult result = execute(cmd);
        if (result.success()) {
            return result.output();
        } else {
            return "ERROR: " + result.errorMessage();
        }
    }

    /**
     * Thực thi đối tượng Command đã parse.
     */
    public synchronized CommandResult execute(Command cmd) {
        if (cmd == null || cmd.type() == CommandType.UNKNOWN) {
            return CommandResult.error("Malformed command");
        }

        return switch (cmd.type()) {
            case SET -> {
                data.put(cmd.key(), cmd.value());
                yield CommandResult.ok("OK");
            }
            case GET -> {
                String val = data.get(cmd.key());
                if (val != null) {
                    yield CommandResult.ok(val);
                } else {
                    yield CommandResult.error("Key not found: " + cmd.key());
                }
            }
            case DEL -> {
                String removed = data.remove(cmd.key());
                if (removed != null) {
                    yield CommandResult.ok("OK");
                } else {
                    yield CommandResult.error("Key not found: " + cmd.key());
                }
            }
            case CAS -> {
                // CAS <key> <expectedValue> <newValue>
                String current = data.get(cmd.key());
                if (Objects.equals(current, cmd.expectedValue())) {
                    data.put(cmd.key(), cmd.value());
                    yield CommandResult.ok("OK");
                } else {
                    yield CommandResult
                            .error("CAS_FAILED: expected '" + cmd.expectedValue() + "' but was '" + current + "'");
                }
            }
            case EXISTS -> {
                boolean exists = data.containsKey(cmd.key());
                yield CommandResult.ok(exists ? "TRUE" : "FALSE");
            }
            case KEYS -> {
                // Hỗ trợ pattern đơn giản: "*" (tất cả) hoặc "prefix*" / "prefix"
                String pattern = cmd.key();
                Set<String> matchedKeys = scanKeys(pattern);
                if (matchedKeys.isEmpty()) {
                    yield CommandResult.ok("(empty)");
                } else {
                    yield CommandResult.ok(String.join(", ", matchedKeys));
                }
            }
            case UNKNOWN -> CommandResult.error("Unknown command type");
        };
    }

    /**
     * Lọc danh sách keys theo pattern tiền tố hoặc "*" (tất cả).
     */
    public Set<String> scanKeys(String pattern) {
        if (pattern == null || pattern.equals("*") || pattern.isBlank()) {
            return Set.copyOf(data.keySet());
        }
        String prefix = pattern.endsWith("*") ? pattern.substring(0, pattern.length() - 1) : pattern;
        return data.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Trả về toàn bộ danh sách key hiện có.
     */
    public Set<String> keys() {
        return Set.copyOf(data.keySet());

    }

    /**
     * Đọc giá trị trực tiếp từ StateMachine (Read-only, không ghi log Raft).
     */
    public String get(String key) {
        return data.get(key);
    }

    /**
     * Kiểm tra key có tồn tại không.
     */
    public boolean exists(String key) {
        return data.containsKey(key);
    }

    /**
     * Trả về số lượng key hiện có trong database.
     */
    public int size() {
        return data.size();
    }

    /**
     * Xóa toàn bộ dữ liệu (dùng khi test hoặc khôi phục snapshot).
     */
    public synchronized void clear() {
        data.clear();
    }

    /**
     * Xuất dữ liệu hiện tại để Dashboard hiển thị hoặc tạo Snapshot.
     */
    public Map<String, String> getSnapshotData() {
        return Map.copyOf(data);
    }

    /**
     * Nạp lại dữ liệu từ Snapshot vào StateMachine.
     */
    public synchronized void restoreSnapshotData(Map<String, String> snapshot) {
        data.clear();
        if (snapshot != null) {
            data.putAll(snapshot);
        }
    }
}

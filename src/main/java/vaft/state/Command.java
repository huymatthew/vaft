package vaft.state;

/**
 * Đại diện cho một câu lệnh thao tác với Key-Value Store.
 * Hỗ trợ các lệnh:
 *  - SET <key> <value> hoặc SET <key>=<value>
 *  - GET <key>
 *  - DEL <key>
 *  - CAS <key> <expectedValue> <newValue>
 *  - EXISTS <key>
 *  - KEYS [pattern] (ví dụ: KEYS, KEYS *, KEYS user:*)
 */
public record Command(
        CommandType type,
        String key,
        String value,
        String expectedValue
) {

    /**
     * Phân tích một chuỗi rawCommand thành đối tượng Command.
     * Tương thích ngược với cú pháp "SET key=value" cũ.
     */
    public static Command parse(String rawCommand) {
        if (rawCommand == null || rawCommand.isBlank()) {
            return new Command(CommandType.UNKNOWN, null, null, null);
        }

        String trimmed = rawCommand.trim();
        String[] parts = trimmed.split("\\s+", 2);
        String op = parts[0].toUpperCase();

        return switch (op) {
            case "SET" -> {
                if (parts.length < 2) {
                    yield new Command(CommandType.UNKNOWN, null, null, null);
                }
                String rest = parts[1];
                if (rest.contains("=")) {
                    String[] kv = rest.split("=", 2);
                    String k = kv[0].trim();
                    String v = kv.length > 1 ? kv[1].trim() : "";
                    yield new Command(CommandType.SET, k, v, null);
                } else {
                    String[] kv = rest.split("\\s+", 2);
                    String k = kv[0].trim();
                    String v = kv.length > 1 ? kv[1].trim() : "";
                    yield new Command(CommandType.SET, k, v, null);
                }
            }
            case "GET" -> parts.length < 2
                    ? new Command(CommandType.UNKNOWN, null, null, null)
                    : new Command(CommandType.GET, parts[1].trim(), null, null);
            case "DEL" -> parts.length < 2
                    ? new Command(CommandType.UNKNOWN, null, null, null)
                    : new Command(CommandType.DEL, parts[1].trim(), null, null);
            case "EXISTS" -> parts.length < 2
                    ? new Command(CommandType.UNKNOWN, null, null, null)
                    : new Command(CommandType.EXISTS, parts[1].trim(), null, null);
            case "KEYS" -> {
                String pattern = (parts.length >= 2 && !parts[1].isBlank()) ? parts[1].trim() : "*";
                yield new Command(CommandType.KEYS, pattern, null, null);
            }
            case "CAS" -> {
                if (parts.length < 2) {
                    yield new Command(CommandType.UNKNOWN, null, null, null);
                }
                String[] casParts = parts[1].split("\\s+", 3);
                if (casParts.length < 3) {
                    yield new Command(CommandType.UNKNOWN, null, null, null);
                }
                String k = casParts[0].trim();
                String expected = casParts[1].trim();
                String newVal = casParts[2].trim();
                yield new Command(CommandType.CAS, k, newVal, expected);
            }
            default -> new Command(CommandType.UNKNOWN, null, null, null);
        };
    }

    /**
     * Chuyển đổi thành chuỗi lệnh chuẩn hóa.
     */
    public String toRawCommand() {
        return switch (type) {
            case SET -> "SET " + key + " " + (value != null ? value : "");
            case GET -> "GET " + key;
            case DEL -> "DEL " + key;
            case CAS -> "CAS " + key + " " + expectedValue + " " + value;
            case EXISTS -> "EXISTS " + key;
            case KEYS -> "KEYS " + (key != null ? key : "*");
            default -> "UNKNOWN";
        };
    }
}

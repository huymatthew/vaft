package vaft.state;

/**
 * Kết quả thực thi một câu lệnh trên StateMachine (Database).
 */
public record CommandResult(
        boolean success,
        String output,
        String errorMessage
) {
    public static CommandResult ok(String output) {
        return new CommandResult(true, output, null);
    }

    public static CommandResult error(String errorMessage) {
        return new CommandResult(false, null, errorMessage);
    }
}


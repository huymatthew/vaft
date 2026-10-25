package vaft.protocol;

public record LogEntry(
        int index,
        int term,
        String command
) {}
package vaft.storage;

import java.util.Map;

public record RaftSnapshot(
        int lastIncludedIndex,
        int lastIncludedTerm,
        Map<String, String> data
) {}

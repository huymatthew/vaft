package vaft.storage;

import java.io.IOException;
import java.util.List;

import vaft.protocol.LogEntry;

public interface PersistenceStorage extends AutoCloseable {
    void appendEntries(List<LogEntry> entries) throws IOException; // nối đuôi + commit
    void truncateFrom(int index) throws IOException;               // cắt log từ index
    void saveMeta(int currentTerm, int votedFor) throws IOException;
    void saveMeta(PersistentState state) throws IOException;
    PersistentState load() throws IOException;                     // meta + log

    default void saveSnapshot(RaftSnapshot snapshot) throws IOException {}
    default RaftSnapshot loadSnapshot() throws IOException { return null; }

    @Override
    default void close() throws IOException {}
}

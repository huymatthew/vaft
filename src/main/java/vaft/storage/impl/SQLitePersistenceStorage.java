package vaft.storage.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import vaft.protocol.LogEntry;
import vaft.storage.PersistenceStorage;
import vaft.storage.PersistentState;
import vaft.storage.RaftSnapshot;

public class SQLitePersistenceStorage implements PersistenceStorage {
    private final Path dbFile;
    private final Connection conn;

    public SQLitePersistenceStorage(Path path) throws IOException {
        if (Files.isDirectory(path) || !path.getFileName().toString().contains(".")) {
            Files.createDirectories(path);
            this.dbFile = path.resolve("raft.db");
        } else {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.dbFile = path;
        }

        String url = "jdbc:sqlite:" + this.dbFile.toAbsolutePath();
        try {
            this.conn = DriverManager.getConnection(url);
            initSchema();
        } catch (SQLException e) {
            throw new IOException("Failed to initialize SQLite storage at " + this.dbFile, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = WAL;");
            stmt.execute("PRAGMA synchronous = NORMAL;");

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS raft_meta (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        current_term INTEGER NOT NULL,
                        voted_for INTEGER NOT NULL
                    );
                    """);

            stmt.execute("""
                    INSERT OR IGNORE INTO raft_meta (id, current_term, voted_for)
                    VALUES (1, 0, -1);
                    """);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS raft_log (
                        log_index INTEGER PRIMARY KEY,
                        term INTEGER NOT NULL,
                        command TEXT
                    );
                    """);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS raft_snapshot_meta (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        last_included_index INTEGER NOT NULL,
                        last_included_term INTEGER NOT NULL
                    );
                    """);

            stmt.execute("""
                    INSERT OR IGNORE INTO raft_snapshot_meta (id, last_included_index, last_included_term)
                    VALUES (1, 0, 0);
                    """);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS raft_snapshot_kv (
                        k TEXT PRIMARY KEY,
                        v TEXT
                    );
                    """);
        }
    }

    @Override
    public synchronized void appendEntries(List<LogEntry> entries) throws IOException {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        String sql = "INSERT OR REPLACE INTO raft_log (log_index, term, command) VALUES (?, ?, ?)";
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (LogEntry entry : entries) {
                    if (entry == null || entry.index() <= 0) {
                        continue;
                    }
                    ps.setInt(1, entry.index());
                    ps.setInt(2, entry.term());
                    ps.setString(3, entry.command());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            throw new IOException("Failed to append entries to SQLite", e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignored) {
            }
        }
    }

    @Override
    public synchronized void truncateFrom(int index) throws IOException {
        if (index <= 0) {
            return;
        }
        String sql = "DELETE FROM raft_log WHERE log_index >= ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, index);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to truncate log from index " + index, e);
        }
    }

    @Override
    public synchronized void saveMeta(int currentTerm, int votedFor) throws IOException {
        String sql = "UPDATE raft_meta SET current_term = ?, voted_for = ? WHERE id = 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, currentTerm);
            ps.setInt(2, votedFor);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save metadata to SQLite", e);
        }
    }

    @Override
    public synchronized void saveMeta(PersistentState state) throws IOException {
        if (state == null) {
            return;
        }
        saveMeta(state.currentTerm(), state.votedFor());
    }

    @Override
    public synchronized PersistentState load() throws IOException {
        int currentTerm = 0;
        int votedFor = -1;

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT current_term, voted_for FROM raft_meta WHERE id = 1")) {
            if (rs.next()) {
                currentTerm = rs.getInt("current_term");
                votedFor = rs.getInt("voted_for");
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load metadata from SQLite", e);
        }

        List<LogEntry> logEntries = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery("SELECT log_index, term, command FROM raft_log ORDER BY log_index ASC")) {
            while (rs.next()) {
                logEntries.add(new LogEntry(
                        rs.getInt("log_index"),
                        rs.getInt("term"),
                        rs.getString("command")));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load log entries from SQLite", e);
        }

        return new PersistentState(currentTerm, votedFor, logEntries);
    }

    @Override
    public synchronized void saveSnapshot(RaftSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            return;
        }
        String updateMetaSql = "UPDATE raft_snapshot_meta SET last_included_index = ?, last_included_term = ? WHERE id = 1";
        String clearKvSql = "DELETE FROM raft_snapshot_kv";
        String insertKvSql = "INSERT INTO raft_snapshot_kv (k, v) VALUES (?, ?)";

        try {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(updateMetaSql)) {
                ps.setInt(1, snapshot.lastIncludedIndex());
                ps.setInt(2, snapshot.lastIncludedTerm());
                ps.executeUpdate();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(clearKvSql);
            }

            if (snapshot.data() != null && !snapshot.data().isEmpty()) {
                try (PreparedStatement ps = conn.prepareStatement(insertKvSql)) {
                    for (Map.Entry<String, String> entry : snapshot.data().entrySet()) {
                        ps.setString(1, entry.getKey());
                        ps.setString(2, entry.getValue());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }

            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            throw new IOException("Failed to save snapshot to SQLite", e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignored) {
            }
        }
    }

    @Override
    public synchronized RaftSnapshot loadSnapshot() throws IOException {
        int lastIncludedIndex = 0;
        int lastIncludedTerm = 0;

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT last_included_index, last_included_term FROM raft_snapshot_meta WHERE id = 1")) {
            if (rs.next()) {
                lastIncludedIndex = rs.getInt("last_included_index");
                lastIncludedTerm = rs.getInt("last_included_term");
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load snapshot metadata from SQLite", e);
        }

        Map<String, String> data = new LinkedHashMap<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT k, v FROM raft_snapshot_kv")) {
            while (rs.next()) {
                data.put(rs.getString("k"), rs.getString("v"));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load snapshot key-value pairs from SQLite", e);
        }

        return new RaftSnapshot(lastIncludedIndex, lastIncludedTerm, data);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new IOException("Failed to close SQLite connection", e);
        }
    }
}

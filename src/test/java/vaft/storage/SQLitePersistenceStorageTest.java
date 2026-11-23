package vaft.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.config.SnapshotConfig;
import vaft.core.NodeRole;
import vaft.core.RaftState;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.LogEntry;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.replication.impl.RaftLogManagerImpl;
import vaft.replication.impl.ReplicationServiceImpl;
import vaft.state.StateMachine;
import vaft.storage.impl.SQLitePersistenceStorage;
import vaft.transport.RaftTransport;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

public class SQLitePersistenceStorageTest {

    @TempDir
    Path tempDir;

    private SQLitePersistenceStorage storage;

    @BeforeEach
    public void setUp() throws Exception {
        storage = new SQLitePersistenceStorage(tempDir.resolve("test-node.db"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    public void testInitialLoadDefaults() throws Exception {
        PersistentState state = storage.load();
        assertEquals(0, state.currentTerm());
        assertEquals(-1, state.votedFor());
        assertTrue(state.log().isEmpty());

        RaftSnapshot snap = storage.loadSnapshot();
        assertEquals(0, snap.lastIncludedIndex());
        assertEquals(0, snap.lastIncludedTerm());
        assertTrue(snap.data().isEmpty());
    }

    @Test
    public void testSaveAndReloadMetaAndLog() throws Exception {
        storage.saveMeta(3, 2);
        storage.appendEntries(List.of(
                new LogEntry(1, 1, "SET a 10"),
                new LogEntry(2, 2, "SET b 20"),
                new LogEntry(3, 3, "SET c 30")
        ));

        PersistentState state = storage.load();
        assertEquals(3, state.currentTerm());
        assertEquals(2, state.votedFor());
        assertEquals(3, state.log().size());
        assertEquals("SET b 20", state.log().get(1).command());

        // Truncate from index 2
        storage.truncateFrom(2);
        PersistentState afterTruncate = storage.load();
        assertEquals(1, afterTruncate.log().size());
        assertEquals(1, afterTruncate.log().get(0).index());
    }

    @Test
    public void testSaveAndLoadSnapshot() throws Exception {
        Map<String, String> kv = Map.of("k1", "v1", "k2", "v2");
        storage.saveSnapshot(new RaftSnapshot(50, 4, kv));

        RaftSnapshot loaded = storage.loadSnapshot();
        assertEquals(50, loaded.lastIncludedIndex());
        assertEquals(4, loaded.lastIncludedTerm());
        assertEquals(2, loaded.data().size());
        assertEquals("v1", loaded.data().get("k1"));
        assertEquals("v2", loaded.data().get("k2"));
    }

    @Test
    public void testAutoSnapshotAfter50Entries() throws Exception {
        RaftState raftState = new RaftState(storage);
        raftState.setCurrentTerm(1);
        raftState.setRole(NodeRole.LEADER);
        raftState.setLeaderId(1);

        RaftLogManagerImpl logManager = new RaftLogManagerImpl(storage);
        StateMachine stateMachine = new StateMachine();

        NodeInfo self = new NodeInfo(1, "localhost", 9001, 8001);
        ClusterConfig config = new ClusterConfig();
        config.setSelf(self);
        config.setNodes(List.of(self));

        SnapshotConfig snapshotConfig = new SnapshotConfig();
        snapshotConfig.setSnapshotThresholdEntries(50);
        config.setSnapshotConfig(snapshotConfig);

        RaftTransport dummyTransport = new RaftTransport() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public RequestVoteResponse sendRequestVote(int targetId, RequestVoteRequest req) { return null; }
            @Override public AppendEntriesResponse sendAppendEntries(int targetId, AppendEntriesRequest req) { return null; }
        };

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ReplicationServiceImpl replicationService = new ReplicationServiceImpl(
                    raftState, logManager, config, dummyTransport,
                    Executors.newFixedThreadPool(1), scheduler, stateMachine,
                    () -> {}, () -> {}, () -> {}
            );

            // Gửi 49 lệnh -> chưa tới ngưỡng 50 nên chưa chụp snapshot
            for (int i = 1; i <= 49; i++) {
                replicationService.submitClientCommand("SET key" + i + " val" + i).get();
            }
            assertEquals(0, storage.loadSnapshot().lastIncludedIndex());

            // Lệnh thứ 50 -> tự động kích hoạt chụp snapshot vào SQLite
            replicationService.submitClientCommand("SET key50 val50").get();

            RaftSnapshot snap = storage.loadSnapshot();
            assertEquals(50, snap.lastIncludedIndex());
            assertEquals(1, snap.lastIncludedTerm());
            assertEquals(50, snap.data().size());
            assertEquals("val50", snap.data().get("key50"));
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void testIntegrationWithRaftStateAndLogManager() throws Exception {
        RaftState raftState = new RaftState(storage);
        RaftLogManagerImpl logManager = new RaftLogManagerImpl(storage);

        raftState.setCurrentTerm(5);
        raftState.setVotedFor(1);
        logManager.appendCommand(5, "SET user:1 Huy");
        logManager.appendCommand(5, "SET user:2 Matthew");

        // Giả lập node sập và khởi động lại trên cùng file DB
        PersistentState recovered = storage.load();
        assertEquals(5, recovered.currentTerm());
        assertEquals(1, recovered.votedFor());
        assertEquals(2, recovered.log().size());

        RaftState newRaftState = new RaftState();
        newRaftState.restoreMeta(recovered.currentTerm(), recovered.votedFor());
        RaftLogManagerImpl newLogManager = new RaftLogManagerImpl(storage);
        newLogManager.restoreEntries(recovered.log());

        assertEquals(5, newRaftState.getCurrentTerm());
        assertEquals(1, newRaftState.getVotedFor());
        assertEquals(2, newLogManager.getLastLogIndex());
        assertEquals("SET user:2 Matthew", newLogManager.getEntry(2).command());
    }
}

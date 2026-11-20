package vaft.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.NodeRole;
import vaft.core.RaftState;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.ClientCommandResult;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.replication.impl.RaftLogManagerImpl;
import vaft.replication.impl.ReplicationServiceImpl;
import vaft.state.StateMachine;
import vaft.transport.RaftTransport;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

public class ReadWritePathTest {

    private RaftState raftState;
    private RaftLogManager logManager;
    private StateMachine stateMachine;
    private ReplicationService replicationService;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() {
        raftState = new RaftState();
        raftState.setCurrentTerm(1);
        raftState.setRole(NodeRole.LEADER);
        raftState.setLeaderId(1);

        logManager = new RaftLogManagerImpl(() -> {});
        stateMachine = new StateMachine();

        NodeInfo self = new NodeInfo(1, "localhost", 9001, 8081);
        ClusterConfig config = new ClusterConfig();
        config.setSelf(self);
        config.setNodes(List.of(self)); // Quorum = 1 để commit ngay lập tức khi chạy đơn node

        RaftTransport dummyTransport = new RaftTransport() {
            @Override
            public void start() {}
            @Override
            public void stop() {}
            @Override
            public RequestVoteResponse sendRequestVote(int targetNodeId, RequestVoteRequest request) {
                return null;
            }
            @Override
            public AppendEntriesResponse sendAppendEntries(int targetNodeId, AppendEntriesRequest request) {
                return null;
            }
        };

        scheduler = Executors.newSingleThreadScheduledExecutor();

        replicationService = new ReplicationServiceImpl(
                raftState,
                logManager,
                config,
                dummyTransport,
                Executors.newFixedThreadPool(2),
                scheduler,
                stateMachine,
                () -> {},
                () -> {},
                () -> {}
        );
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    public void testWritePathAppendsLogAndCommits() throws Exception {
        int initialLogIndex = logManager.getLastLogIndex(); // Thường là 0 (Sentinel)

        // 1. Gửi lệnh GHI: SET user:1 Tuan
        CompletableFuture<ClientCommandResult> future = replicationService.submitClientCommand("SET user:1 Tuan");
        ClientCommandResult result = future.get();

        // 2. Kiểm tra phản hồi Client
        assertTrue(result.accepted());
        assertEquals("COMMITTED", result.status());
        assertEquals("OK", result.stateMachineOutput());
        assertEquals(initialLogIndex + 1, result.logIndex());

        // 3. Kiểm tra Log đã được ghi vào RaftLog
        assertEquals(initialLogIndex + 1, logManager.getLastLogIndex());
        assertEquals("SET user:1 Tuan", logManager.getEntry(result.logIndex()).command());

        // 4. Kiểm tra dữ liệu đã được Apply vào State Machine
        assertEquals("Tuan", stateMachine.get("user:1"));
    }

    @Test
    public void testReadPathDoesNotAppendLog() throws Exception {
        // Chuẩn bị dữ liệu ban đầu thông qua StateMachine
        stateMachine.apply("SET user:2 Alice");
        int logIndexBefore = logManager.getLastLogIndex();

        // 1. Thao tác GET
        CompletableFuture<ClientCommandResult> getFuture = replicationService.submitClientCommand("GET user:2");
        ClientCommandResult getResult = getFuture.get();

        assertTrue(getResult.accepted());
        assertEquals("SUCCESS", getResult.status());
        assertEquals("Alice", getResult.stateMachineOutput());
        assertEquals(-1, getResult.logIndex(), "Read Path không được sinh ra log index");
        assertEquals(logIndexBefore, logManager.getLastLogIndex(), "Log không được tăng thêm entry");

        // 2. Thao tác KEYS
        CompletableFuture<ClientCommandResult> keysFuture = replicationService.submitClientCommand("KEYS");
        ClientCommandResult keysResult = keysFuture.get();

        assertTrue(keysResult.accepted());
        assertTrue(keysResult.stateMachineOutput().contains("user:2"));
        assertEquals(logIndexBefore, logManager.getLastLogIndex(), "KEYS không được tăng thêm entry vào log");

        // 3. Thao tác EXISTS
        CompletableFuture<ClientCommandResult> existsFuture = replicationService.submitClientCommand("EXISTS user:2");
        ClientCommandResult existsResult = existsFuture.get();

        assertTrue(existsResult.accepted());
        assertEquals("TRUE", existsResult.stateMachineOutput());
        assertEquals(logIndexBefore, logManager.getLastLogIndex(), "EXISTS không được tăng thêm entry vào log");
    }

    @Test
    public void testRejectWhenNotLeader() throws Exception {
        // Đổi trạng thái sang Follower
        raftState.setRole(NodeRole.FOLLOWER);
        raftState.setLeaderId(2);

        // Gửi lệnh ghi khi không phải Leader
        CompletableFuture<ClientCommandResult> writeFuture = replicationService.submitClientCommand("SET k v");
        ClientCommandResult writeResult = writeFuture.get();
        assertFalse(writeResult.accepted());
        assertEquals("NOT_LEADER", writeResult.status());
        assertEquals(2, writeResult.leaderId());

        // Gửi lệnh đọc khi không phải Leader
        CompletableFuture<ClientCommandResult> readFuture = replicationService.submitClientCommand("GET k");
        ClientCommandResult readResult = readFuture.get();
        assertFalse(readResult.accepted());
        assertEquals("NOT_LEADER", readResult.status());
        assertEquals(2, readResult.leaderId());
    }

    @Test
    public void testMalformedCommandRejected() throws Exception {
        int logIndexBefore = logManager.getLastLogIndex();

        // Lệnh vô nghĩa không hợp lệ
        CompletableFuture<ClientCommandResult> future = replicationService.submitClientCommand("INVALID_OP something");
        ClientCommandResult result = future.get();

        assertFalse(result.accepted());
        assertTrue(result.status().startsWith("MALFORMED_COMMAND"));
        assertEquals(logIndexBefore, logManager.getLastLogIndex(), "Lệnh lỗi không được ghi vào Raft Log");
    }
}

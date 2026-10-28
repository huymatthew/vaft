package vaft;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import vaft.bootstrap.RaftNode;
import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.core.RaftState;
import vaft.election.ElectionService;
import vaft.election.impl.ElectionServiceImpl;
import vaft.replication.RaftLogManager;
import vaft.replication.RaftLogView;
import vaft.replication.ReplicationService;
import vaft.replication.impl.TestLog;
import vaft.replication.impl.TestLogManager;
import vaft.replication.impl.TestReplicationService;
import vaft.transport.NetworkPartitionTable;
import vaft.transport.RaftRpcHandler;
import vaft.transport.RaftTransport;
import vaft.transport.TcpServer;
import vaft.transport.impl.RaftRcpHandlerImpl;
import vaft.transport.impl.TcpRaftTransport;
import vaft.utils.UltiMethod;
public class App {
    public static RaftNode raftNode;
    public static TcpServer tcpServer;
    public static void main(String[] args) throws Exception {
        int selfNodeId = UltiMethod.getNodeIdFromArgs(args);
        ClusterConfig config = ClusterConfig.loadFromFile(UltiMethod.getConfigPathFromArgs(args), selfNodeId);

        NodeInfo selfNode = config.getSelf();
        RaftState raftState = new RaftState();

        NetworkPartitionTable partitionTable = new NetworkPartitionTable();
        // Duration connectTimeout = Duration.ofMillis(networkConfig.getConnectTimeoutMs());
        // Duration soTimeout = Duration.ofMillis(networkConfig.getRequestTimeoutMs());

        RaftRpcHandler rpcHandler = new RaftRcpHandlerImpl();
        RaftTransport transport = new TcpRaftTransport(config, rpcHandler, partitionTable);

        // Leader Election Service
        RaftLogView logView = new TestLog();
        ExecutorService rpcExecutor = Executors.newFixedThreadPool(4);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        RaftLogManager logManager = new TestLogManager();
        AtomicReference<ElectionService> electionServiceHolder =
                new AtomicReference<>();
        ReplicationService replicationService = new TestReplicationService(
                raftState,
                logManager,
                config,
                transport,
                scheduler,
                () -> electionServiceHolder.get().resetElectionTimer());
        ElectionService electionService = new ElectionServiceImpl(raftState, logView, config, transport, rpcExecutor, scheduler, replicationService);
        electionServiceHolder.set(electionService);
        
        // ReplicationService replicationService = new ReplicationService();
        App.raftNode = new RaftNode(config, selfNode, raftState, transport, electionService, replicationService, null);
        ((RaftRcpHandlerImpl) rpcHandler).setService(raftNode);
        System.out.println("Hello, this is a Vaft node. " + "My ID is " + selfNode.getId() + " and my address is " + selfNode.getHost() + ":" + selfNode.getPort() + ".");
        transport.start();

        Thread.sleep(ThreadLocalRandom.current().nextLong(2000, 5001));
        transport.sendRequestVote(selfNodeId % 3 + 1, new vaft.protocol.RequestVoteRequest(1, 1, 1, 1));

        // Docker shutdown hook để dừng server khi container bị dừng
        CountDownLatch keepAliveLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Vaft node...");
            tcpServer.stop();
            keepAliveLatch.countDown();
        }));

        // 3. Giữ tiến trình chạy mãi mãi cho đến khi bị tắt
        keepAliveLatch.await();
    }
}
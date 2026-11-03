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
import vaft.replication.ReplicationService;
import vaft.replication.impl.RaftLogManagerImpl;
import vaft.replication.impl.ReplicationServiceImpl;
import vaft.state.StateMachine;
import vaft.transport.NetworkPartitionTable;
import vaft.transport.RaftRpcHandler;
import vaft.transport.RaftTransport;
import vaft.transport.TcpServer;
import vaft.transport.impl.RaftRcpHandlerImpl;
import vaft.transport.impl.TcpRaftTransport;
import vaft.utils.UltiMethod;
import vaft.dashboard.DashboardAdminServer;
import vaft.dashboard.impl.DashboardAdminServerImpl;

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
        ExecutorService rpcExecutor = Executors.newFixedThreadPool(4);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        
        RaftLogManager logManager = new RaftLogManagerImpl(() -> {
            System.out.println("Save State to DB");
        });
        StateMachine stateMachine = new StateMachine();
        AtomicReference<ElectionService> electionServiceHolder = new AtomicReference<>();
        ReplicationService replicationService = new ReplicationServiceImpl(
                raftState,
                logManager,
                config,
                transport,
                rpcExecutor,
                scheduler,
                stateMachine,
                () -> {},
                () -> {},
                () -> electionServiceHolder.get().resetElectionTimer());
        
        ElectionService electionService = new ElectionServiceImpl(
                raftState,
                logManager,
                config,
                transport,
                rpcExecutor,
                scheduler,
                replicationService,
                partitionTable);
        electionServiceHolder.set(electionService);


        DashboardAdminServer dashboardServer = new DashboardAdminServerImpl(
                selfNode.getId(),
                selfNode.getAdminPort(),
                raftState,
                logManager,
                stateMachine,
                partitionTable,
                replicationService
        );

        dashboardServer.start();

        // ReplicationService replicationService = new ReplicationService();
        App.raftNode = new RaftNode(config, selfNode, raftState, transport, electionService, replicationService, dashboardServer);
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
            dashboardServer.close();
            transport.stop();
        }));

        // 3. Giữ tiến trình chạy mãi mãi cho đến khi bị tắt
        keepAliveLatch.await();
    }
}

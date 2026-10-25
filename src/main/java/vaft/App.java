package vaft;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import vaft.bootstrap.RaftNode;
import vaft.config.ClusterConfig;
import vaft.config.ElectionConfig;
import vaft.config.NetworkConfig;
import vaft.config.NodeInfo;
import vaft.core.RaftState;
import vaft.transport.NetworkPartitionTable;
import vaft.transport.RaftRpcHandler;
import vaft.transport.RaftTransport;
import vaft.transport.TcpServer;
import vaft.transport.impl.TcpRaftTransport;
import vaft.transport.impl.TestRaftRpcHandler;
import vaft.utils.UltiMethod;
public class App {
    public static RaftNode raftNode;
    public static TcpServer tcpServer;
    public static void main(String[] args) throws Exception {
        int selfNodeId = UltiMethod.getNodeIdFromArgs(args);
        ClusterConfig config = ClusterConfig.loadFromFile(UltiMethod.getConfigPathFromArgs(args), selfNodeId);
        NetworkConfig networkConfig = config.getNetworkConfig();
        ElectionConfig electionConfig = config.getElectionConfig();

        NodeInfo selfNode = config.getSelf();
        RaftState raftState = new RaftState();

        NetworkPartitionTable partitionTable = new NetworkPartitionTable();
        // Duration connectTimeout = Duration.ofMillis(networkConfig.getConnectTimeoutMs());
        // Duration soTimeout = Duration.ofMillis(networkConfig.getRequestTimeoutMs());

        RaftRpcHandler rpcHandler = new TestRaftRpcHandler();
        RaftTransport transport = new TcpRaftTransport(config, rpcHandler, partitionTable);

        // ElectionService electionService = new ElectionService(electionConfig);
        
        // ReplicationService replicationService = new ReplicationService();
        App.raftNode = new RaftNode(config, selfNode, raftState, transport, null, null, null);
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
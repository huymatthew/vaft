package vaft;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;

import vaft.bootstrap.RaftNode;
import vaft.config.ClusterConfig;
import vaft.config.ElectionConfig;
import vaft.config.NetworkConfig;
import vaft.config.NodeInfo;
import vaft.core.RaftState;
import vaft.transport.NetworkPartitionTable;
import vaft.transport.RaftTransport;
import vaft.transport.impl.TcpRaftTransport;
import vaft.utils.UltiMethod;
public class App {
    public static RaftNode raftNode;
    public static void main(String[] args) {
        String selfNodeId = UltiMethod.getNodeIdFromArgs(args);
        ClusterConfig config = ClusterConfig.loadFromFile(UltiMethod.getConfigPathFromArgs(args), selfNodeId);
        NetworkConfig networkConfig = config.getNetworkConfig();
        ElectionConfig electionConfig = config.getElectionConfig();

        NodeInfo selfNode = config.getSelf();
        RaftState raftState = new RaftState();

        NetworkPartitionTable partitionTable = new NetworkPartitionTable();
        Duration connectTimeout = Duration.ofMillis(networkConfig.getConnectTimeoutMs());
        Duration soTimeout = Duration.ofMillis(networkConfig.getRequestTimeoutMs());
        RaftTransport transport = new TcpRaftTransport(selfNode, config.getPeers(), partitionTable, connectTimeout, soTimeout);

        // ElectionService electionService = new ElectionService(electionConfig);
        
        // ReplicationService replicationService = new ReplicationService();

        App.raftNode = new RaftNode(config, selfNode, raftState, transport, null, null, null);
        System.out.println("Hello, this is a Vaft node. " + "My ID is " + selfNode.getId() + " and my address is " + selfNode.getHost() + ":" + selfNode.getPort() + ".");
        startNode();
    }

    private static void startNode() {
        try {
            ServerSocket sc = new ServerSocket(raftNode.getSelf().getPort());
            while (true) {
                Socket socket = sc.accept();
                new ServerThread(socket).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class ServerThread extends Thread {
    private Socket socket;

    public ServerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            int message;
            while ((message = dis.read()) != -1) {
                System.out.println("Received a message from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
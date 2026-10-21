package vaft;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import vaft.config.ClusterConfig;
import vaft.config.NodeInfo;
import vaft.utils.UltiMethod;


public class App {
    public static ClusterConfig config;
    public static NodeInfo selfNode;
    public static void main(String[] args) {
        ClusterConfig config = ClusterConfig.loadFromFile(UltiMethod.getConfigPathFromArgs(args));
        App.config = config;
        selfNode = config.getNodeById(UltiMethod.getNodeIdFromArgs(args));
        System.out.println("Hello, this is a Vaft node. " + "My ID is " + selfNode.getId() + " and my address is " + selfNode.getHost() + ":" + selfNode.getPort() + ".");
        startNode();
    }

    private static void startNode() {
        try {
            ServerSocket sc = new ServerSocket(selfNode.getPort());
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
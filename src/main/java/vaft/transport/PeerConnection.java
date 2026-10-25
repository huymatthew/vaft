package vaft.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import vaft.config.NodeInfo;
import vaft.protocol.RpcEnvelope;

public class PeerConnection {
    private final NodeInfo targetPeer;
    private final int connectTimeoutMs = 100;
    private final int readTimeoutMs = 150;

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;

    public PeerConnection(NodeInfo targetPeer) {
        this.targetPeer = targetPeer;
    }

    /**
     * Khóa synchronized trên từng PeerConnection:
     * Đảm bảo các gói tin gửi tới CÙNG 1 PEER đi tuần tự trên 1 TCP stream mở sẵn,
     * trong khi các Peer khác nhau vẫn chạy song song 100%.
     */
    public synchronized RpcEnvelope sendAndReceive(RpcEnvelope request) {
        try {
            ensureConnected();
            
            // Ghi gói tin và đọc phản hồi ngay trên stream đang mở sẵn
            PacketCodec.writePacket(out, request);
            return PacketCodec.readPacket(in);

        } catch (IOException e) {
            // Nếu node đích sập, timeout hoặc đứt mạng -> Hủy socket cũ để lần sau tự reconnect
            closeQuietly();
            return null;
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null && !socket.isClosed() && socket.isConnected()) {
            return;
        }
        closeQuietly();
        socket = new Socket();
        socket.setTcpNoDelay(true); 
        socket.setKeepAlive(true);
        socket.connect(new InetSocketAddress(targetPeer.getHost(), targetPeer.getPort()), connectTimeoutMs);
        socket.setSoTimeout(readTimeoutMs);

        // Khởi tạo BufferedStream ĐÚNG 1 LẦN khi mở Socket
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    }

    public synchronized void closeQuietly() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        } finally {
            socket = null;
            out = null;
            in = null;
        }
    }
}
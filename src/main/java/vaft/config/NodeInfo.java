package vaft.config;

public class NodeInfo {
    private int id;
    private String host;
    private int port;
    private int adminPort;
    // 1. Bắt buộc có Constructor rỗng cho SnakeYAML
    public NodeInfo() {}

    // Bạn vẫn có thể giữ lại constructor này nếu cần dùng ở nơi khác
    public NodeInfo(int id, String host, int port, int adminPort) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.adminPort = adminPort;
    }

    // 2. Thêm các Setter để SnakeYAML map dữ liệu
    public void setId(int  id) { this.id = id; }
    public void setHost(String host) { this.host = host; }
    public void setPort(int port) { this.port = port; }
    public void setAdminPort(int adminPort) { this.adminPort = adminPort; }
    public int getAdminPort() { return adminPort; }

    public int getId() { return id; }
    public String getHost() { return host; }
    public int getPort() { return port; }

    public int getNodeId() { return id; }
    public int getRaftTcpPort() { return port; }
}
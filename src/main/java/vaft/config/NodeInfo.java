package vaft.config;

public class NodeInfo {
    private String id;
    private String host;
    private int port;

    // 1. Bắt buộc có Constructor rỗng cho SnakeYAML
    public NodeInfo() {}

    // Bạn vẫn có thể giữ lại constructor này nếu cần dùng ở nơi khác
    public NodeInfo(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    // 2. Thêm các Setter để SnakeYAML map dữ liệu
    public void setId(String id) { this.id = id; }
    public void setHost(String host) { this.host = host; }
    public void setPort(int port) { this.port = port; }

    public String getId() { return id; }
    public String getHost() { return host; }
    public int getPort() { return port; }

    public String getNodeId() { return id; }
    public int getRaftTcpPort() { return port; }
    public int getDashboardHttpPort() { return port; }
}
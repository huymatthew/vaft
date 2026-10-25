package vaft.config;

public class NetworkConfig {
    private int connectTimeoutMs = 1000;
    private int requestTimeoutMs = 2000;

    public NetworkConfig() {}
    // Getters and Setters
    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }
    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }
    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }
    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }
}
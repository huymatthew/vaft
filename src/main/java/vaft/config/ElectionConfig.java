package vaft.config;

public class ElectionConfig {
    // Giá trị mặc định chuẩn cho Raft
    private long heartbeatIntervalMs = 150;
    private long minTimeoutMs = 300;
    private long maxTimeoutMs = 600;

    public ElectionConfig() {}
    // Getters and Setters
    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long getMinTimeoutMs() {
        return minTimeoutMs;
    }

    public long getMaxTimeoutMs() {
        return maxTimeoutMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public void setMinTimeoutMs(long minTimeoutMs) {
        this.minTimeoutMs = minTimeoutMs;
    }

    public void setMaxTimeoutMs(long maxTimeoutMs) {
        this.maxTimeoutMs = maxTimeoutMs;
    }
}
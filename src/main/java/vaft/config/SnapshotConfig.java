package vaft.config;

public class SnapshotConfig {
    private int snapshotThresholdEntries = 50;

    public SnapshotConfig() {}

    public int getSnapshotThresholdEntries() {
        return snapshotThresholdEntries;
    }

    public void setSnapshotThresholdEntries(int snapshotThresholdEntries) {
        this.snapshotThresholdEntries = snapshotThresholdEntries;
    }
}

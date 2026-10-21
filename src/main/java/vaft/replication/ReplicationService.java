package vaft.replication;

import vaft.protocol.*;

public interface ReplicationService {
    void onBecomeLeader();
    void onHeartbeatTick();
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
    boolean submitClientCommand(String command);
}

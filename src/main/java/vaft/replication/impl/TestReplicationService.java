package vaft.replication.impl;

import java.util.concurrent.CompletableFuture;

import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.ClientCommandResult;
import vaft.replication.ReplicationService;

public class TestReplicationService implements ReplicationService {
    @Override
    public void onBecomeLeader() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }

    @Override
    public void onStepDown() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }

    @Override
    public void onHeartbeatTick() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        return null;
    }

    @Override
    public CompletableFuture<ClientCommandResult> submitClientCommand(String command) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void advanceCommitIndexIfPossible() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }

    @Override
    public void applyCommittedEntries() {
        // Không thực hiện gì trong phiên bản thử nghiệm.
    }
}

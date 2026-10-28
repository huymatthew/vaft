package vaft.transport.impl;
import vaft.bootstrap.RaftNode;
import vaft.election.ElectionService;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.replication.ReplicationService;
import vaft.transport.RaftRpcHandler;

public class RaftRcpHandlerImpl implements RaftRpcHandler {
    private ElectionService electionService;
    private ReplicationService replicationService;
    public void setService(RaftNode raftNode) {
        this.electionService = raftNode.getElectionService();
        this.replicationService = raftNode.getReplicationService();
    }
    @Override
    public RequestVoteResponse onRequestVote(RequestVoteRequest request) {
        return electionService.handleRequestVote(request);
    }

    @Override
    public AppendEntriesResponse onAppendEntries(
            AppendEntriesRequest request) {
        return replicationService.handleAppendEntries(request);
    }
    
}

package vaft.transport.impl;
import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.transport.RaftRpcHandler;
public class TestRaftRpcHandler implements RaftRpcHandler {
    @Override 
    public RequestVoteResponse onRequestVote(RequestVoteRequest request){
        System.out.println("Received RequestVote: " + request);
        return new RequestVoteResponse();
    }
    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest request){
        System.out.println("Received AppendEntries: " + request);
        return new AppendEntriesResponse();
    }
    
}

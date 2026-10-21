package vaft.transport;

import vaft.protocol.*;

public interface RaftRpcHandler {
    RequestVoteResponse onRequestVote(RequestVoteRequest request);
    AppendEntriesResponse onAppendEntries(AppendEntriesRequest request);
}

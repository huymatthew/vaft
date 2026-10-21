package vaft.transport;

import vaft.protocol.*;

public interface RaftTransport {
    RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest request);
    AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest request);
}

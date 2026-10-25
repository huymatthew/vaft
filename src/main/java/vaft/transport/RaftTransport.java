package vaft.transport;

import java.io.IOException;

import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;

public interface RaftTransport {
    RequestVoteResponse sendRequestVote(int targetNodeId, RequestVoteRequest request);
    AppendEntriesResponse sendAppendEntries(int targetNodeId, AppendEntriesRequest request);
    void start() throws IOException;
    void stop();
}

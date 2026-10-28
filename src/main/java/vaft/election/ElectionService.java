package vaft.election;

import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;

public interface ElectionService {
    void onElectionTimeout();
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);
    void resetElectionTimer();
}

package vaft.election;

import vaft.protocol.*;

public interface ElectionService {
    void onElectionTimeout();
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);
}

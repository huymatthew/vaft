package vaft.protocol;
import java.util.List;

public class AppendEntriesRequest {
    private int term;
    private int leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private List<LogEntry> entries = java.util.List.of();
    private int leaderCommit;

    public AppendEntriesRequest(int term, String leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public AppendEntriesRequest() {}
    public AppendEntriesRequest(int term, int leaderId, int prevLogIndex, int prevLogTerm, java.util.List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
    public int getTerm() { return term; }
    public void setTerm(int term) { this.term = term; }
    public int getLeaderId() { return leaderId; }
    public void setLeaderId(int leaderId) { this.leaderId = leaderId; }
    public int getPrevLogIndex() { return prevLogIndex; }
    public void setPrevLogIndex(int value) { prevLogIndex = value; }
    public int getPrevLogTerm() { return prevLogTerm; }
    public void setPrevLogTerm(int value) { prevLogTerm = value; }
    public List<LogEntry> getEntries() { return entries; }
    public void setEntries(List<LogEntry> entries) { this.entries = entries; }
    public int getLeaderCommit() { return leaderCommit; }
    public void setLeaderCommit(int leaderCommit) { this.leaderCommit = leaderCommit; }
}

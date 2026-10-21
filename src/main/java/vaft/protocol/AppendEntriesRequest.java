package vaft.protocol;

public class AppendEntriesRequest {
    private int term;
    private String leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private java.util.List<LogEntry> entries = java.util.List.of();
    private int leaderCommit;

    public AppendEntriesRequest() {}
    public int getTerm() { return term; }
    public void setTerm(int term) { this.term = term; }
    public String getLeaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public int getPrevLogIndex() { return prevLogIndex; }
    public void setPrevLogIndex(int value) { prevLogIndex = value; }
    public int getPrevLogTerm() { return prevLogTerm; }
    public void setPrevLogTerm(int value) { prevLogTerm = value; }
    public java.util.List<LogEntry> getEntries() { return entries; }
    public void setEntries(java.util.List<LogEntry> entries) { this.entries = entries; }
    public int getLeaderCommit() { return leaderCommit; }
    public void setLeaderCommit(int leaderCommit) { this.leaderCommit = leaderCommit; }
}

package vaft.protocol;

public class AppendEntriesResponse {
    private int term;
    private boolean success;
    private int matchIndex;
    private int conflictIndex;

    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term, boolean success, int matchIndex, int conflictIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = conflictIndex;
    }
    public int getTerm() { return term; }
    public void setTerm(int term) { this.term = term; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public int getMatchIndex() { return matchIndex; }
    public void setMatchIndex(int matchIndex) { this.matchIndex = matchIndex; }
    public int getConflictIndex() { return conflictIndex; }
    public void setConflictIndex(int conflictIndex) { this.conflictIndex = conflictIndex; }
}

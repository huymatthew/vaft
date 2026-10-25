package vaft.protocol;

public class AppendEntriesResponse {
    private int term;
    private boolean success;// true nếu Follower khớp prevLogIndex và prevLogTerm
    private int matchIndex;// Chỉ số log cao nhất đã khớp trên Follower (dùng khi success = true)
    private int conflictIndex;// Chỉ số gợi ý để Leader lùi nextIndex nhanh (dùng khi success = false)
    private int conflictTerm; // Term của entry bị xung đột tại prevLogIndex (0 nếu Follower thiếu log)

    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term, boolean success, int matchIndex, int conflictIndex, int conflictTerm) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = conflictIndex;
        this.conflictTerm = conflictTerm;
    }
    public int getTerm() { return term; }
    public void setTerm(int term) { this.term = term; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public int getMatchIndex() { return matchIndex; }
    public void setMatchIndex(int matchIndex) { this.matchIndex = matchIndex; }
    public int getConflictIndex() { return conflictIndex; }
    public void setConflictIndex(int conflictIndex) { this.conflictIndex = conflictIndex; }
    public int getConflictTerm() { return conflictTerm; }
    public void setConflictTerm(int conflictTerm) { this.conflictTerm = conflictTerm;
    }

    public static AppendEntriesResponse ok(int term, int matchIndex) {
        return new AppendEntriesResponse(term, true, matchIndex, -1, -1);
    }

    public static AppendEntriesResponse fail(int term, int conflictIndex, int conflictTerm) {
        return new AppendEntriesResponse(term, false, -1, conflictIndex, conflictTerm);
    }
}

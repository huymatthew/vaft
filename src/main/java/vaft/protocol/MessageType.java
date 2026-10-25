package vaft.protocol;

public enum MessageType {
    REQUEST_VOTE_REQ,
    REQUEST_VOTE_RES,
    APPEND_ENTRIES_REQ,
    APPEND_ENTRIES_RES;

    public static MessageType fromCode(int code) {
        return values()[code];
    }
    public int toCode() {
        return this.ordinal();
    }
}

package vaft.state;

public enum CommandType {
    SET,
    GET,
    DEL,
    CAS,
    EXISTS,
    KEYS,
    UNKNOWN;

    /**
     * Kiểm tra xem lệnh này có phải là lệnh chỉ đọc (Read-only) hay không.
     * Các lệnh chỉ đọc không làm thay đổi trạng thái dữ liệu (State Machine),
     * do đó có thể thực thi trực tiếp tại Leader mà không cần thông qua Raft Log Quorum.
     */
    public boolean isReadOnly() {
        return this == GET || this == KEYS || this == EXISTS;
    }

    /**
     * Kiểm tra xem lệnh này có phải là lệnh ghi (Write) hay không.
     * Các lệnh ghi làm biến đổi State Machine, bắt buộc phải ghi Raft Log,
     * nhân bản sang đa số Follower (Quorum) trước khi áp dụng.
     */
    public boolean isWrite() {
        return this == SET || this == DEL || this == CAS;
    }
}

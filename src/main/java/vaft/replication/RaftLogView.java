package vaft.replication;

import java.util.List;

import vaft.protocol.LogEntry;

public interface RaftLogView {
    /**
     * Trả về index của phần tử cuối cùng trong log (Trả về 0 nếu log rỗng, chỉ có Sentinel).
     */
    int getLastLogIndex();

    /**
     * Trả về term của phần tử cuối cùng trong log (Trả về 0 nếu log rỗng).
     */
    int getLastLogTerm();

    /**
     * Lấy term tại một vị trí index bất kỳ (Trả về -1 nếu index vượt quá độ dài log hiện tại).
     */
    int getTermAt(int index);

    /**
     * Lấy một LogEntry tại vị trí index (index >= 0).
     */
    LogEntry getEntry(int index);

    /**
     * Kiểm tra xem log của Candidate (trong RequestVoteRequest) có "mới bằng hoặc mới hơn"
    log của node hiện tại hay không:
     * - So sánh lastLogTerm trước.
     * - Nếu cùng term thì ai có lastLogIndex >= người đó mới hơn.
     */
    boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm);

    /**
     * Trả về bản sao bất biến toàn bộ các LogEntry (từ index 1..lastLogIndex)
     * để đóng gói vào NodeStatusSnapshot có thể dùng để phục vụ giao diện UI.
     */
    List<LogEntry> getAllEntriesSnapshot();
}

package vaft.replication;

import java.util.List;
import vaft.protocol.LogEntry;

public interface RaftLogManager extends RaftLogView {

    /**
     * (Chỉ dùng trên Leader) Tạo mới một LogEntry ở cuối log với currentTerm và command,
     * lưu bền vững xuống PersistenceStorage và trả về LogEntry vừa tạo.
     */
    LogEntry appendCommand(int currentTerm, String command);

    /**
     * Lấy danh sách các LogEntry từ startIndex đến hết log hiện tại
     * để đóng gói vào AppendEntriesRequest gửi cho Follower (tương ứng từ nextIndex[peerId]).
     * Trả về List rỗng nếu startIndex > getLastLogIndex().
     */
    List<LogEntry> getEntriesFrom(int startIndex);

    /**
     * Kiểm tra điều kiện khớp nối log tại Follower :
       Trả về true nếu log hiện tại có chứa một entry tại prevLogIndex
     * mà entry.term() == prevLogTerm. (Với prevLogIndex == 0 thì luôn trả về true).
     */
    //Hiểu đơn giản là để kiểm tra xem log của follower có khớp với log của leader tại vị trí 
    // prevLogIndex hay không. Nếu follower có entry tại prevLogIndex và term của entry đó bằng prevLogTerm, 
    // thì log được coi là khớp. Nếu prevLogIndex là 0, thì luôn trả về true vì đó là entry Sentinel.
    // nếu không thì trả về false, nghĩa là follower không có entry tại prevLogIndex h
    // hoặc term của entry đó không khớp với prevLogTerm. Khi đó, leader sẽ cần phải điều chỉnh 
    // nextIndex của follower để tìm vị trí log khớp và gửi lại các entries từ vị trí đó.
    boolean matchLog(int prevLogIndex, int prevLogTerm);

    /**
     * (Dùng trên Follower khi nhận AppendEntries hợp lệ)
     * - Duyệt qua danh sách newEntries bắt đầu sau prevLogIndex.
     * - Nếu tại một vị trí đã có entry nhưng KHÁC term (xung đột với Leader mới),
     *   cắt bỏ (truncate) từ vị trí đó đến hết log, rồi nối phần còn lại của newEntries vào.
     * - Lưu ý: Nếu newEntries là rỗng (Heartbeat) hoặc đã nằm trọn trong log hiện tại,
     *   TUYỆT ĐỐI KHÔNG được cắt bớt các entry phía sau của Follower (phòng trường hợp gói tin đến trễ).
     */
    void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries);

    /**
     * (Dùng trên Follower khi nhận AppendEntries hợp lệ)
     * Overload có thêm tham số commitIndex để bảo vệ an toàn:
     * Tuyệt đối không bao giờ truncate các entry đã commit (targetIndex <= commitIndex).
     */
    default void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries, int commitIndex) {
        appendEntriesFromLeader(prevLogIndex, newEntries);
    }

    /**
     * Hỗ trợ tối ưu Fast Backup khi matchLog(prevLogIndex, prevLogTerm) trả về false:
     * (Dùng trên Follower) Tìm chỉ số ĐẦU TIÊN trong log có cùng term với entry bị xung đột tại prevLogIndex.
     */
    int findFirstIndexOfTerm(int conflictTerm, int maxSearchIndex);

    /**
     * Hỗ trợ tối ưu Fast Backup khi nhận AppendEntriesResponse bị reject (conflictTerm > 0):
     * (Dùng trên Leader) Tìm chỉ số CUỐI CÙNG trong log của Leader có cùng conflictTerm
     * để Leader có thể nhảy tới nextIndex = lastIndexOfTerm + 1.
     * Trả về -1 nếu Leader không chứa entry nào thuộc conflictTerm.
     */
    default int findLastIndexOfTerm(int conflictTerm, int maxSearchIndex) {
        return -1;
    }

    /**
     * Khôi phục trạng thái log từ danh sách log lưu trữ bền vững (Persistence Storage).
     */
    default void restoreEntries(List<LogEntry> recoveredEntries) {
    }
}
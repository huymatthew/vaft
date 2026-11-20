package vaft.replication.impl;

import vaft.protocol.LogEntry;
import vaft.replication.RaftLogManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RaftLogManagerImpl implements RaftLogManager {

    private final List<LogEntry> log;

    // Callback để tự động gọi PersistenceStorage.saveState() khi log thay đổi
    private final Runnable saveStateCallback;

    public RaftLogManagerImpl(Runnable saveStateCallback) {
        this(Collections.emptyList(), saveStateCallback);
    }

    /**
     * Khởi tạo RaftLogManager với dữ liệu log đã được nạp từ PersistenceStorage khi
     * Node khởi động lại.
     */
    public RaftLogManagerImpl(List<LogEntry> initialLog, Runnable saveStateCallback) {
        this.log = new ArrayList<>();
        this.saveStateCallback = saveStateCallback != null ? saveStateCallback : () -> {
        };
        restoreEntries(initialLog);
    }

    /**
     * Khôi phục trạng thái log từ danh sách log lưu trữ bền vững (Persistence
     * Storage).
     * Đảm bảo Sentinel Entry luôn cố định ở index 0 và các entry được nạp đúng thứ
     * tự.
     */
    @Override
    public synchronized void restoreEntries(List<LogEntry> recoveredEntries) {
        this.log.clear();
        // Luôn khởi tạo Sentinel Entry tại index 0
        this.log.add(new LogEntry(0, 0, null));

        if (recoveredEntries == null || recoveredEntries.isEmpty()) {
            return;
        }

        for (LogEntry entry : recoveredEntries) {
            if (entry == null) {
                continue;
            }
            if (entry.index() <= 0) {
                // Bỏ qua sentinel entry nếu đã có sẵn trong danh sách lưu trữ
                continue;
            }
            this.log.add(entry);
        }
    }

    // ==========================================
    // 1. CÁC HÀM ĐỌC TỪ INTERFACE RaftLogView (Thread-Safe)
    // ==========================================

    @Override
    public synchronized int getLastLogIndex() {
        return Math.max(0, log.size() - 1);
    }

    @Override
    public synchronized int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(getLastLogIndex()).term();
    }

    @Override
    public synchronized int getTermAt(int index) {
        if (index < 0 || index >= log.size()) {
            return -1;
        }
        return log.get(index).term();
    }

    @Override
    public synchronized LogEntry getEntry(int index) {
        if (index < 0 || index >= log.size()) {
            return null;
        }
        return log.get(index);
    }

    @Override
    public synchronized boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = this.getLastLogTerm();
        int myLastLogIndex = this.getLastLogIndex();

        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }

    @Override
    public synchronized List<LogEntry> getAllEntriesSnapshot() {
        if (log.size() <= 1) {
            return Collections.emptyList();
        }
        return List.copyOf(log.subList(1, log.size()));
    }

    // ==========================================
    // 2. CÁC HÀM GHI TỪ INTERFACE RaftLogManager (Thread-Safe)
    // ==========================================

    @Override
    public synchronized LogEntry appendCommand(int currentTerm, String command) {
        int nextIndex = getLastLogIndex() + 1;
        LogEntry newEntry = new LogEntry(nextIndex, currentTerm, command);
        log.add(newEntry);

        // Bắt buộc gọi lưu xuống đĩa ngay sau khi append
        saveStateCallback.run();

        return newEntry;
    }

    @Override
    public synchronized List<LogEntry> getEntriesFrom(int startIndex) {
        // Sentinel Entry nằm ở index 0; log entries thực tế bắt đầu từ index 1
        int actualStart = Math.max(1, startIndex);
        if (actualStart > getLastLogIndex()) {
            return Collections.emptyList();
        }
        // Trả về bản sao để quá trình gửi mạng TCP không gặp lỗi ConcurrentModification
        return List.copyOf(log.subList(actualStart, log.size()));
    }

    @Override
    public synchronized boolean matchLog(int prevLogIndex, int prevLogTerm) {
        if (prevLogIndex < 0) {
            return false;
        }
        if (prevLogIndex == 0) {
            return true; // Sentinel entry luôn luôn khớp
        }
        if (prevLogIndex > getLastLogIndex()) {
            return false; // Follower bị thiếu log
        }
        return getTermAt(prevLogIndex) == prevLogTerm;
    }

    @Override
    public synchronized void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries) {
        appendEntriesFromLeader(prevLogIndex, newEntries, 0);
    }

    @Override
    public synchronized void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries, int commitIndex) {
        if (newEntries == null || newEntries.isEmpty() || prevLogIndex < 0) {
            return; // Bản tin Heartbeat hoặc prevLogIndex không hợp lệ (< 0)
        }

        // Đảm bảo Sentinel Entry luôn luôn tồn tại ở index 0
        if (log.isEmpty()) {
            log.add(new LogEntry(0, 0, null));
        }

        boolean modified = false;

        for (int i = 0; i < newEntries.size(); i++) {
            LogEntry entry = newEntries.get(i);
            int targetIndex = prevLogIndex + 1 + i;

            // BẢO VỆ SENTINEL ENTRY: Index 0 là bất biến, không bao giờ được ghi đè hoặc
            // cắt bỏ
            if (targetIndex < 1) {
                continue;
            }

            // BẢO VỆ LOG ĐÃ COMMIT: Không bao giờ truncate entry có targetIndex <=
            // commitIndex
            // Raft Invariant (Leader Completeness & State Machine Safety):
            // Các entry đã commit là bất biến. Nếu có xung đột ở targetIndex <=
            // commitIndex, từ chối truncate!
            if (targetIndex <= commitIndex) {
                if (getTermAt(targetIndex) != entry.term()) {
                    // Phát hiện xung đột với entry đã commit: Tuyệt đối không truncate, dừng xử lý
                    // ngay
                    return;
                }
                // Nếu term giống nhau: Entry đã commit và trùng khớp, an toàn bỏ qua để duyệt
                // tiếp
                continue;
            }

            if (targetIndex <= getLastLogIndex()) {
                // Đã có entry ở vị trí này (chưa commit), cần kiểm tra term
                if (getTermAt(targetIndex) != entry.term()) {
                    // PHÁT HIỆN XUNG ĐỘT TRÊN LOG CHƯA COMMIT:
                    // Cắt bỏ (truncate) từ targetIndex đến hết log hiện tại
                    // targetIndex > commitIndex >= 0 đảm bảo không bao giờ xóa nhầm Sentinel Entry
                    // hoặc entry đã commit
                    log.subList(targetIndex, log.size()).clear();

                    // Nối toàn bộ log từ leader kể từ điểm xung đột
                    log.addAll(newEntries.subList(i, newEntries.size()));
                    modified = true;
                    break;
                }
                // Nếu term giống nhau -> Bỏ qua (coi như đã có sẵn, không làm gì cả).
                // Điều này cực kỳ quan trọng để xử lý lỗi gói tin TCP bị gửi lặp/đến trễ.
            } else {
                // Follower đã hết log cũ, chỉ việc nối thêm tất cả phần còn lại
                log.addAll(newEntries.subList(i, newEntries.size()));
                modified = true;
                break;
            }
        }

        if (modified) {
            saveStateCallback.run(); // Chỉ ghi xuống đĩa khi thực sự có thay đổi
        }
    }

    @Override
    public synchronized int findFirstIndexOfTerm(int conflictTerm, int maxSearchIndex) {
        int firstIndex = -1;
        int searchLimit = Math.min(maxSearchIndex, getLastLogIndex());

        // Quét ngược từ vị trí xung đột về đầu mảng
        for (int i = searchLimit; i > 0; i--) {
            if (getTermAt(i) == conflictTerm) {
                firstIndex = i;
            } else if (firstIndex != -1) {
                // Đã tìm thấy dải term bị xung đột và vừa lọt qua term trước đó -> Dừng lại
                break;
            }
        }

        return firstIndex;
    }

    @Override
    public synchronized int findLastIndexOfTerm(int conflictTerm, int maxSearchIndex) {
        int searchLimit = Math.min(maxSearchIndex, getLastLogIndex());
        // Quét ngược từ maxSearchIndex về 1; entry đầu tiên khớp chính là index lớn
        // nhất (cuối cùng) của term đó
        for (int i = searchLimit; i > 0; i--) {
            if (getTermAt(i) == conflictTerm) {
                return i;
            }
        }
        return -1;
    }
}
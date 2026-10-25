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
        this.log = new ArrayList<>();
        // Khởi tạo Sentinel Entry tại index 0
        this.log.add(new LogEntry(0, 0, null));
        this.saveStateCallback = saveStateCallback != null ? saveStateCallback : () -> {};
    }

    // ==========================================
    // 1. CÁC HÀM ĐỌC TỪ INTERFACE RaftLogView
    // ==========================================
    
    @Override
    public int getLastLogIndex() {
        return log.size() - 1;
    }

    @Override
    public int getLastLogTerm() {
        return log.get(getLastLogIndex()).term();
    }

    @Override
    public int getTermAt(int index) {
        if (index < 0 || index >= log.size()) {
            return -1;
        }
        return log.get(index).term();
    }

    @Override
    public LogEntry getEntry(int index) {
        if (index < 0 || index >= log.size()) {
            return null;
        }
        return log.get(index);
    }

    @Override
    public boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = this.getLastLogTerm();
        int myLastLogIndex = this.getLastLogIndex();

        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }

    @Override
    public List<LogEntry> getAllEntriesSnapshot() {
        if (log.size() <= 1) {
            return Collections.emptyList();
        }
        return List.copyOf(log.subList(1, log.size()));
    }

    // ==========================================
    // 2. CÁC HÀM GHI TỪ INTERFACE RaftLogManager
    // ==========================================

    @Override
    public LogEntry appendCommand(int currentTerm, String command) {
        int nextIndex = getLastLogIndex() + 1;
        LogEntry newEntry = new LogEntry(nextIndex, currentTerm, command);
        log.add(newEntry);
        
        // Bắt buộc gọi lưu xuống đĩa ngay sau khi append
        saveStateCallback.run();
        
        return newEntry;
    }

    @Override
    public List<LogEntry> getEntriesFrom(int startIndex) {
        if (startIndex > getLastLogIndex()) {
            return Collections.emptyList();
        }
        // Trả về bản sao để quá trình gửi mạng TCP không gặp lỗi ConcurrentModification
        return List.copyOf(log.subList(startIndex, log.size()));
    }

    @Override
    public boolean matchLog(int prevLogIndex, int prevLogTerm) {
        if (prevLogIndex == 0) {
            return true; // Sentinel entry luôn luôn khớp
        }
        if (prevLogIndex > getLastLogIndex()) {
            return false; // Follower bị thiếu log
        }
        return getTermAt(prevLogIndex) == prevLogTerm;
    }

    @Override
    public void appendEntriesFromLeader(int prevLogIndex, List<LogEntry> newEntries) {
        if (newEntries == null || newEntries.isEmpty()) {
            return; // Đây chỉ là bản tin Heartbeat, không có log mới
        }

        boolean modified = false;

        for (int i = 0; i < newEntries.size(); i++) {
            LogEntry entry = newEntries.get(i);
            int targetIndex = prevLogIndex + 1 + i;

            if (targetIndex <= getLastLogIndex()) {
                // Đã có entry ở vị trí này, cần kiểm tra term
                if (getTermAt(targetIndex) != entry.term()) {
                    // PHÁT HIỆN XUNG ĐỘT: Cắt bỏ (truncate) từ targetIndex đến hết log hiện tại
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
    public int findFirstIndexOfTerm(int conflictTerm, int maxSearchIndex) {
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
}
package vaft.protocol;

public record ClientCommandResult(
        boolean accepted,   // true nếu lệnh đã được commit bởi Quorum và áp dụng vào FSM
        String status,      // "COMMITTED", "NOT_LEADER", "TIMEOUT"
        String leaderId,    // Địa chỉ của Leader hiện tại để Client tự redirect
        int logIndex,       // Vị trí của lệnh trong Raft Log
        int logTerm,        // Nhiệm kỳ (term) của lệnh
        String stateMachineOutput // Dữ liệu trả về từ Database
) {}
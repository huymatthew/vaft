package vaft.dashboard;

import java.util.List;
import java.util.Map;

import vaft.core.NodeRole;
import vaft.protocol.LogEntry;

public record NodeStatusSnapshot(
                int nodeId,
                NodeRole role,
                int currentTerm,
                String votedFor,
                int leaderId,
                int commitIndex,
                List<LogEntry> log,
                Map<String, String> stateMachine,
                boolean networkIsolated) {
        /**
         * Chuyển đổi trạng thái Node thành chuỗi JSON để gửi cho trình duyệt Web
         */
        public String toJson() {
                StringBuilder sb = new StringBuilder();
                sb.append("{");
                sb.append("\"nodeId\":").append(nodeId).append(",");
                sb.append("\"role\":\"").append(role != null ? role.name() : "UNKNOWN").append("\",");
                sb.append("\"currentTerm\":").append(currentTerm).append(",");
                sb.append("\"votedFor\":").append(votedFor != null ? "\"" + votedFor + "\"" : "null").append(",");
                sb.append("\"leaderId\":").append(leaderId).append(",");
                sb.append("\"commitIndex\":").append(commitIndex).append(",");
                sb.append("\"networkIsolated\":").append(networkIsolated).append(",");

                // Chuyển đổi danh sách log entries thành mảng JSON
                sb.append("\"log\":[");
                if (log != null && !log.isEmpty()) {
                        for (int i = 0; i < log.size(); i++) {
                                LogEntry entry = log.get(i);
                                sb.append("{\"index\":").append(entry.index())
                                                .append(",\"term\":").append(entry.term())
                                                .append(",\"command\":")
                                                .append(entry.command() != null
                                                                ? "\"" + escapeJson(entry.command()) + "\""
                                                                : "null")
                                                .append("}");
                                if (i < log.size() - 1)
                                        sb.append(",");
                        }
                }
                sb.append("],");

                // Chuyển đổi dữ liệu StateMachine (Key-Value)
                sb.append("\"stateMachine\":{");
                if (stateMachine != null && !stateMachine.isEmpty()) {
                        int count = 0;
                        int total = stateMachine.size();
                        for (Map.Entry<String, String> e : stateMachine.entrySet()) {
                                sb.append("\"").append(escapeJson(e.getKey())).append("\":\"")
                                                .append(escapeJson(e.getValue())).append("\"");
                                if (++count < total)
                                        sb.append(",");
                        }
                }
                sb.append("}");

                sb.append("}");
                return sb.toString();
        }

        private static String escapeJson(String s) {
                if (s == null)
                        return "";
                return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
}

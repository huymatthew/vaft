package vaft.dashboard;

import java.util.List;
import java.util.Map;

import vaft.config.NodeInfo;
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
                boolean networkIsolated,
                List<NodeInfo> clusterNodes,
                Map<Integer, Integer> matchIndex) {
        /**
         * Chuyển đổi trạng thái toàn cụm thành JSON cho giao diện Web
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

                // 1. Danh sách các node trong toàn cụm
                sb.append("\"clusterNodes\":[");
                if (clusterNodes != null && !clusterNodes.isEmpty()) {
                        for (int i = 0; i < clusterNodes.size(); i++) {
                                NodeInfo node = clusterNodes.get(i);
                                sb.append("{\"id\":").append(node.getId())
                                                .append(",\"host\":\"").append(node.getHost()).append("\"")
                                                .append(",\"port\":").append(node.getPort()).append("}");
                                if (i < clusterNodes.size() - 1)
                                        sb.append(",");
                        }
                }
                sb.append("],");

                // 2. Trạng thái matchIndex (tiến độ đồng bộ từ Leader)
                sb.append("\"matchIndex\":{");
                if (matchIndex != null && !matchIndex.isEmpty()) {
                        int count = 0;
                        int total = matchIndex.size();
                        for (Map.Entry<Integer, Integer> e : matchIndex.entrySet()) {
                                sb.append("\"").append(e.getKey()).append("\":").append(e.getValue());
                                if (++count < total)
                                        sb.append(",");
                        }
                }
                sb.append("},");

                // 3. Danh sách log entries
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

                // 4. Dữ liệu StateMachine (Key-Value)
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

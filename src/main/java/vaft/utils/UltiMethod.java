package vaft.utils;

public class UltiMethod {
    public static int  getNodeIdFromArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--nodeId=")) {
                String nodeIdStr = arg.substring("--nodeId=".length());
                return Integer.parseInt(nodeIdStr);
            }
        }
        return -1; // Return -1 if nodeId is not found
    }
    public static String getConfigPathFromArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--config=")) {
                return arg.substring("--config=".length());
            }
        }
        return null; // Return null if config path is not found
    }
}

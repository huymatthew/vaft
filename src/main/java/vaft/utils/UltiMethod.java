package vaft.utils;

public class UltiMethod {
    public static String getNodeIdFromArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--nodeId=")) {
                return arg.substring("--nodeId=".length());
            }
        }
        return null; // Return null if nodeId is not found
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

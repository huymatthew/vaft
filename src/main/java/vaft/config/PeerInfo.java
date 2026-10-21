package vaft.config;

public record PeerInfo(String nodeId, String host, int raftTcpPort, int dashboardHttpPort) {}

package vaft.config;

import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class ClusterConfig {
    private List<NodeInfo> nodes;
    private NodeInfo self;
    private ElectionConfig electionConfig;
    private NetworkConfig networkConfig;
    // 1. Constructor rỗng cho SnakeYAML
    public ClusterConfig() {}

    // 3. Chuyển logic đọc file thành Factory Method (hàm static)
    public static ClusterConfig loadFromFile(String configFilePath, int selfNodeId) {
        try {
            Yaml yaml = new Yaml(new Constructor(ClusterConfig.class, new LoaderOptions()));
            FileInputStream inputStream = new FileInputStream(configFilePath);
            ClusterConfig config = yaml.loadAs(inputStream, ClusterConfig.class);
            config.setSelf(config.getNodeById(selfNodeId));
            return config;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    // 2. Setter để SnakeYAML gán danh sách nodes vào
    public void setNodes(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    public List<NodeInfo> getNodes() {
        return nodes;
    }

    public void setSelf(NodeInfo self) {
        this.self = self;
    }

    public NodeInfo getNodeById(int nodeId) {
        if (nodes != null) {
            for (NodeInfo node : nodes) {
                if (node.getId() == nodeId) {
                    return node;
                }
            }
        }
        return null; // Return null if node with the given ID is not found
    }

    public NodeInfo getSelf() {
        return self;
    }

    public List<NodeInfo> getPeersList() {
        return nodes == null ? List.of() : nodes.stream()
                .filter(node -> !Objects.equals(node.getId(), self.getId()))
                .collect(Collectors.toUnmodifiableList());
    }

    public Map<Integer, NodeInfo> getPeers() {
        return nodes == null ? Map.of() : nodes.stream()
                .filter(node -> !Objects.equals(node.getId(), self.getId()))
                .collect(Collectors.toUnmodifiableMap(NodeInfo::getId, node -> node));
    }

    public int getQuorumSize() {
        return nodes == null ? 0 : nodes.size() / 2 + 1;
    }

    public ElectionConfig getElectionConfig() {
        return electionConfig;
    }

    public void setElectionConfig(ElectionConfig electionConfig) {
        this.electionConfig = electionConfig;
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public void setNetworkConfig(NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
    }
}
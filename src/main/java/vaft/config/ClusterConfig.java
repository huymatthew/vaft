package vaft.config;

import java.io.FileInputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class ClusterConfig {
    private List<NodeInfo> nodes;
    private NodeInfo self;

    // 1. Constructor rỗng cho SnakeYAML
    public ClusterConfig() {}

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

    // 3. Chuyển logic đọc file thành Factory Method (hàm static)
    public static ClusterConfig loadFromFile(String configFilePath) {
        try {
            Yaml yaml = new Yaml(new Constructor(ClusterConfig.class, new LoaderOptions()));
            FileInputStream inputStream = new FileInputStream(configFilePath);
            return yaml.loadAs(inputStream, ClusterConfig.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public NodeInfo getNodeById(String nodeId) {
        if (nodes != null) {
            for (NodeInfo node : nodes) {
                if (node.getId().equals(nodeId)) {
                    return node;
                }
            }
        }
        return null; // Return null if node with the given ID is not found
    }

    public NodeInfo getSelf() {
        return self;
    }

    public List<NodeInfo> getPeers(String selfNodeId) {
        return nodes == null ? List.of() : nodes.stream()
                .filter(node -> !Objects.equals(node.getId(), selfNodeId))
                .collect(Collectors.toUnmodifiableList());
    }

    public int getQuorumSize() {
        return nodes == null ? 0 : nodes.size() / 2 + 1;
    }
}
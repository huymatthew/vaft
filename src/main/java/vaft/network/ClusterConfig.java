package vaft.network;

import java.io.FileInputStream;
import java.util.List;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.Constructor;

public class ClusterConfig {
    private List<NodeInfo> nodes;

    // 1. Constructor rỗng cho SnakeYAML
    public ClusterConfig() {}

    // 2. Setter để SnakeYAML gán danh sách nodes vào
    public void setNodes(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    public List<NodeInfo> getNodes() {
        return nodes;
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
}
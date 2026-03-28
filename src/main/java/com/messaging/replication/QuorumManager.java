package com.messaging.replication;
import com.messaging.common.NodeInfo;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
public class QuorumManager {
    private final int clusterSize;
    private final Map<String, Boolean> nodeStatus = new ConcurrentHashMap<>();
    public QuorumManager(List<NodeInfo> nodes) {
        this.clusterSize = nodes.size();
        nodes.forEach(n -> nodeStatus.put(n.getNodeId(), true));
    }
    public int getWriteQuorum() {
        return (clusterSize / 2) + 1;
    }
    public int getReadQuorum() {
        return (clusterSize / 2) + 1;
    }
    public int getClusterSize() {
        return clusterSize;
    }
    public int getAliveCount() {
        return (int) nodeStatus.values()
                .stream()
                .filter(v -> v)
                .count();
    }
    public boolean hasWriteQuorum() {
        return getAliveCount() >= getWriteQuorum();
    }
    public boolean hasReadQuorum() {
        return getAliveCount() >= getReadQuorum();
    }
    public void markNode(String nodeId, boolean alive) {
        nodeStatus.put(nodeId, alive);
    }
    public boolean isAlive(String nodeId) {
        return nodeStatus.getOrDefault(nodeId, false);
    }
}
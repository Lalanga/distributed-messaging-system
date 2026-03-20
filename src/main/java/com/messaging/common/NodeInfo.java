package com.messaging.common;

import com.messaging.common.enums.NodeRole;

import java.io.Serializable;
import java.util.Objects;

public class NodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String nodeId;
    private String host;
    private int port;
    private NodeRole role;
    private volatile long lastHeartbeat;
    private volatile boolean alive;

    public NodeInfo() {
    }

    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.role = NodeRole.FOLLOWER;
        this.lastHeartbeat = System.currentTimeMillis();
        this.alive = true;
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public NodeRole getRole() { return role; }
    public void setRole(NodeRole role) { this.role = role; }

    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public boolean isAlive() { return alive; }
    public void setAlive(boolean alive) { this.alive = alive; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeInfo)) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(nodeId, nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return String.format("NodeInfo{id='%s', host='%s', port=%d, role=%s}",
                nodeId, host, port, role);
    }
}
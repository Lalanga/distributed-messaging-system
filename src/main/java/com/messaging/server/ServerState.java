package com.messaging.server;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.NodeRole;

import java.util.concurrent.atomic.AtomicReference;

public class ServerState {

    private final NodeInfo localNode;
    private final AtomicReference<NodeRole> role;
    private volatile String leaderId;
    private volatile boolean ready = false;

    public ServerState(NodeInfo localNode) {
        this.localNode = localNode;
        this.role = new AtomicReference<>(NodeRole.FOLLOWER);
    }

    public NodeRole getRole() {
        return role.get();
    }

    public void setRole(NodeRole r) {
        role.set(r);
        localNode.setRole(r);
    }

    public boolean isLeader() {
        return role.get() == NodeRole.LEADER;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String id) {
        leaderId = id;
    }

    public boolean isReady() {
        return ready;
    }

    public void setReady(boolean r) {
        ready = r;
    }

    public NodeInfo getLocalNode() {
        return localNode;
    }
}
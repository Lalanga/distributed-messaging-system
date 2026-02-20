package com.messaging.core;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class ServerConfig {
    private final String serverId;
    private final int port;
    private final Map<String, String> peerAddresses;
    private final int quorumSize;
    private final long heartbeatInterval;
    private final long electionTimeout;
    private final int replicationFactor;

    private ServerConfig(Builder builder) {
        this.serverId = builder.serverId;
        this.port = builder.port;
        this.peerAddresses = builder.peerAddresses;
        this.quorumSize = builder.quorumSize;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.electionTimeout = builder.electionTimeout;
        this.replicationFactor = builder.replicationFactor;
    }

    public static class Builder {
        private String serverId;
        private int port;
        private Map<String, String> peerAddresses = new HashMap<>();
        private int quorumSize = 3;
        private long heartbeatInterval = 1000;
        private long electionTimeout = 5000;
        private int replicationFactor = 2;

        public Builder withServerId(String serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder addPeer(String peerId, String address) {
            this.peerAddresses.put(peerId, address);
            return this;
        }

        public Builder withQuorumSize(int quorumSize) {
            this.quorumSize = quorumSize;
            return this;
        }

        public Builder withHeartbeatInterval(long interval) {
            this.heartbeatInterval = interval;
            return this;
        }

        public Builder withElectionTimeout(long timeout) {
            this.electionTimeout = timeout;
            return this;
        }

        public Builder withReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public ServerConfig build() {
            return new ServerConfig(this);
        }
    }

    // Getters
    public String getServerId() { return serverId; }
    public int getPort() { return port; }
    public Map<String, String> getPeerAddresses() { return peerAddresses; }
    public int getQuorumSize() { return quorumSize; }
    public long getHeartbeatInterval() { return heartbeatInterval; }
    public long getElectionTimeout() { return electionTimeout; }
    public int getReplicationFactor() { return replicationFactor; }
}

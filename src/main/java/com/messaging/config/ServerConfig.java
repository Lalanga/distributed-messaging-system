package com.messaging.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.messaging.common.NodeInfo;
import com.messaging.util.Constants;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class ServerConfig {

    private String serverId;
    private int port = Constants.DEFAULT_PORT;
    private List<NodeInfo> peers = Collections.emptyList();
    private String dataDir = "data";
    private int heartbeatInterval = Constants.HEARTBEAT_INTERVAL_MS;
    private int electionTimeout = Constants.ELECTION_TIMEOUT_BASE;
    private int replicationFactor = Constants.DEFAULT_REPLICATION_FACTOR;
    private String consistencyModel = "QUORUM";
    private boolean enableTimeSync = true;
    private String timeSyncProtocol = "NTP";

    // Getters
    public String getServerId() {
        return serverId;
    }

    public int getPort() {
        return port;
    }

    public List<NodeInfo> getPeers() {
        return peers;
    }

    public String getDataDir() {
        return dataDir;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public int getElectionTimeout() {
        return electionTimeout;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getConsistencyModel() {
        return consistencyModel;
    }

    public boolean isEnableTimeSync() {
        return enableTimeSync;
    }

    public String getTimeSyncProtocol() {
        return timeSyncProtocol;
    }

    // Setters
    public void setServerId(String s) {
        serverId = s;
    }

    public void setPort(int p) {
        port = p;
    }

    public void setPeers(List<NodeInfo> p) {
        peers = p;
    }

    public void setDataDir(String d) {
        dataDir = d;
    }

    public void setHeartbeatInterval(int h) {
        heartbeatInterval = h;
    }

    public void setElectionTimeout(int e) {
        electionTimeout = e;
    }

    public void setReplicationFactor(int r) {
        replicationFactor = r;
    }

    public void setConsistencyModel(String m) {
        consistencyModel = m;
    }

    public void setEnableTimeSync(boolean e) {
        enableTimeSync = e;
    }

    public void setTimeSyncProtocol(String p) {
        timeSyncProtocol = p;
    }

    public static ServerConfig load(String path) throws IOException {
        try (FileReader reader = new FileReader(path)) {
            return new Gson().fromJson(reader, ServerConfig.class);
        }
    }

    public void save(String path) throws IOException {
        try (FileWriter writer = new FileWriter(path)) {
            new GsonBuilder().setPrettyPrinting().create().toJson(this, writer);
        }
    }
}
package com.messaging.consensus.raft;

import com.messaging.common.enums.NodeRole;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Raft §5.4 — currentTerm and votedFor must survive crashes.
 * Persisted in a small properties file for crash recovery.
 */
public class RaftState {

    private static final Logger logger = LoggerUtil.getLogger(RaftState.class);

    private final String nodeId;
    private final Path stateFile;
    private final ReentrantLock stateLock = new ReentrantLock();

    // Persistent state (must be flushed before responding to RPCs)
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;

    // Volatile state
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;
    private volatile long lastHeartbeatMs = System.currentTimeMillis();

    private final int electionTimeoutMs;
    private final int heartbeatIntervalMs;

    public RaftState(String nodeId, int electionTimeoutMs, String dataDir) {
        this.nodeId = nodeId;
        this.electionTimeoutMs = electionTimeoutMs;
        this.heartbeatIntervalMs = electionTimeoutMs / 3;
        this.stateFile = Paths.get(dataDir, nodeId + "-raft.properties");
        loadPersistentState();
    }

    // Persistent getters / setters

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long term) {
        stateLock.lock();
        try {
            this.currentTerm = term;
            this.votedFor = null; // reset vote on new term (§5.1)
            persist();
        } finally {
            stateLock.unlock();
        }
    }

    public long incrementTerm() {
        stateLock.lock();
        try {
            currentTerm++;
            votedFor = null;
            persist();
            return currentTerm;
        } finally {
            stateLock.unlock();
        }
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String candidateId) {
        stateLock.lock();
        try {
            this.votedFor = candidateId;
            persist();
        } finally {
            stateLock.unlock();
        }
    }

    // Volatile getters / setters

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole r) {
        logger.info("[{}] Role transition {} -> {}", nodeId, role, r);
        role = r;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String id) {
        leaderId = id;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long idx) {
        commitIndex = idx;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long idx) {
        lastApplied = idx;
    }

    public void updateLastHeartbeat() {
        lastHeartbeatMs = System.currentTimeMillis();
    }

    public long getLastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public boolean isElectionTimeout() {
        return System.currentTimeMillis() - lastHeartbeatMs > electionTimeoutMs;
    }

    // Persistence helpers

    private void persist() {
        Properties p = new Properties();
        p.setProperty("currentTerm", Long.toString(currentTerm));
        p.setProperty("votedFor", votedFor == null ? "" : votedFor);

        try (OutputStream os = Files.newOutputStream(stateFile,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            p.store(os, "raft-state");
        } catch (IOException e) {
            logger.error("Failed to persist Raft state: {}", e.getMessage());
        }
    }

    private void loadPersistentState() {
        if (!Files.exists(stateFile)) return;

        Properties p = new Properties();
        try (InputStream is = Files.newInputStream(stateFile)) {
            p.load(is);
            currentTerm = Long.parseLong(p.getProperty("currentTerm", "0"));
            String vf = p.getProperty("votedFor", "");
            votedFor = vf.isEmpty() ? null : vf;
            logger.info("[{}] Restored Raft state: term={}, votedFor={}", nodeId, currentTerm, votedFor);
        } catch (IOException | NumberFormatException e) {
            logger.warn("Could not load Raft state from disk, starting fresh: {}", e.getMessage());
        }
    }
}
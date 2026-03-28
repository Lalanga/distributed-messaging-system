package com.messaging.fault;

import com.messaging.common.NodeInfo;
import com.messaging.util.Constants;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;


public class FailureDetector {
    private static final Logger logger = LoggerUtil.getLogger(FailureDetector.class);
    private final NodeInfo localNode;
    private final Map<String, NodeRecord> nodes = new ConcurrentHashMap<>();
    private final int heartbeatMs;
    private final long failThreshMs;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "failure-detector");
                        t.setDaemon(true);
                        return t;
                    }
            );
    private Consumer<String> onFailure; // callback(nodeId)
    private Consumer<String> onRecovery; // callback(nodeId)

    public FailureDetector(NodeInfo localNode, List<NodeInfo> peers, int heartbeatIntervalMs) {
        this.localNode = localNode;
        this.heartbeatMs = heartbeatIntervalMs;
        this.failThreshMs = (long) heartbeatIntervalMs * Constants.MISSED_HEARTBEATS_THRESHOLD;
        peers.forEach(p -> nodes.put(p.getNodeId(), new NodeRecord(p)));
    }

    public void setOnFailure(Consumer<String> cb) { this.onFailure = cb; }
    public void setOnRecovery(Consumer<String> cb) { this.onRecovery = cb; }

    public void start() {
        scheduler.scheduleAtFixedRate(this::check, heartbeatMs, heartbeatMs,
                TimeUnit.MILLISECONDS);
        logger.info(
                "[{}] FailureDetector started (threshold={}ms)",
                localNode.getNodeId(),
                failThreshMs
        );
    }

    public void shutdown() { scheduler.shutdown(); }


    public void recordHeartbeat(String nodeId) {
        NodeRecord rec = nodes.get(nodeId);
        if (rec == null) return;

        boolean wasDown = !rec.alive;
        rec.lastSeenMs = System.currentTimeMillis();
        rec.alive = true;
        rec.info.setAlive(true);
        rec.info.setLastHeartbeat(rec.lastSeenMs);

        if (wasDown) {
            logger.info("[{}] Node {} has recovered", localNode.getNodeId(), nodeId);
            if (onRecovery != null) {
                onRecovery.accept(nodeId);
            }
        }
    }

    public boolean isAlive(String nodeId) {
        NodeRecord rec = nodes.get(nodeId);
        return rec != null && rec.alive;
    }

    public Set<String> getDeadNodes() {
        Set<String> dead = new HashSet<>();
        nodes.forEach((id, rec) -> {
            if (!rec.alive) {
                dead.add(id);
            }
        });
        return dead;
    }

    private void check() {
        long now = System.currentTimeMillis();
        nodes.forEach((nodeId, rec) -> {
            boolean wasAlive = rec.alive;
            boolean timedOut = (now - rec.lastSeenMs) > failThreshMs;

            if (wasAlive && timedOut) {
                rec.alive = false;
                rec.info.setAlive(false);
                logger.warn(
                        "[{}] Node {} presumed FAILED (no heartbeat for {}ms)",
                        localNode.getNodeId(),
                        nodeId,
                        now - rec.lastSeenMs
                );
                if (onFailure != null) {
                    onFailure.accept(nodeId);
                }
            }
        });
    }

    private static class NodeRecord {
        final NodeInfo info;
        volatile long lastSeenMs;
        volatile boolean alive;

        NodeRecord(NodeInfo info) {
            this.info = info;
            this.lastSeenMs = System.currentTimeMillis();
            this.alive = true;
        }
    }
}
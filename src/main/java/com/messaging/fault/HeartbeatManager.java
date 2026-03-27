package com.messaging.fault;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.network.ConnectionManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.*;


public class HeartbeatManager {
    private static final Logger logger = LoggerUtil.getLogger(HeartbeatManager.class);
    private final NodeInfo localNode;
    private final List<NodeInfo> peers;
    private final int intervalMs;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "heartbeat-sender");
                        t.setDaemon(true);
                        return t;
                    });
    private ConnectionManager connManager;
    private volatile boolean running = false;

    public HeartbeatManager(NodeInfo localNode, List<NodeInfo> peers, int intervalMs) {
        this.localNode = localNode;
        this.peers = peers;
        this.intervalMs = intervalMs;
    }

    public void setConnectionManager(ConnectionManager cm) {
        this.connManager = cm;
    }

    public void start() {
        running = true;
        scheduler.scheduleAtFixedRate(this::sendAll, intervalMs, intervalMs,
                TimeUnit.MILLISECONDS);
        logger.info("[{}] HeartbeatManager started (interval={}ms)",
                localNode.getNodeId(), intervalMs);
    }

    public void shutdown() {
        running = false;
        scheduler.shutdown();
    }

    private void sendAll() {
        if (!running || connManager == null) return;
        for (NodeInfo peer : peers) {
            sendHeartbeat(peer);
        }
    }

    private void sendHeartbeat(NodeInfo peer) {
        try {
            HeartbeatPayload payload = new HeartbeatPayload(
                    localNode.getNodeId(),
                    System.currentTimeMillis()
            );
            MessageProtocol msg = new MessageProtocol(
                    MessageType.HEARTBEAT,
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    payload
            );
            connManager.sendMessage(peer.getNodeId(), msg);
        } catch (IOException e) {
            logger.debug(
                    "[{}] Heartbeat to {} failed: {}",
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    e.getMessage()
            );
        }
    }

    public void sendAck(String targetNodeId, long originalTimestamp) {
        if (connManager == null) return;
        try {
            HeartbeatPayload ack = new HeartbeatPayload(
                    localNode.getNodeId(),
                    originalTimestamp
            );
            MessageProtocol msg = new MessageProtocol(
                    MessageType.HEARTBEAT_ACK,
                    localNode.getNodeId(),
                    targetNodeId,
                    ack
            );
            connManager.sendMessageAsync(targetNodeId, msg);
        } catch (Exception e) {
            logger.debug("Heartbeat ACK send error: {}", e.getMessage());
        }
    }

    public static final class HeartbeatPayload implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String senderId;
        public final long sentAtMs;

        public HeartbeatPayload(String senderId, long sentAtMs) {
            this.senderId = senderId;
            this.sentAtMs = sentAtMs;
        }
    }
}
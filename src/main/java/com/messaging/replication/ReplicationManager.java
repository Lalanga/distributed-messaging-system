package com.messaging.replication;
import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.network.ConnectionManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.storage.MessageStore;
import com.messaging.util.Constants;
import com.messaging.util.logger.LoggerUtil;
import com.messaging.util.logger.PerformanceLogger;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
public class ReplicationManager {
    private static final Logger logger =
            LoggerUtil.getLogger(ReplicationManager.class);
    private final NodeInfo localNode;
    private final MessageStore store;
    private final int replicationFactor;
    private final String consistencyModel;
    private final QuorumManager quorumManager;
    private final Map<String, NodeInfo> replicas = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private ConnectionManager connManager;
    public ReplicationManager(NodeInfo localNode, MessageStore store,
                              List<NodeInfo> peers,
                              int replicationFactor, String consistencyModel) {
        this.localNode = localNode;
        this.store = store;
        this.replicationFactor = replicationFactor;
        this.consistencyModel = consistencyModel;
        this.quorumManager = new QuorumManager(peers);
        peers.forEach(p -> replicas.put(p.getNodeId(), p));
    }
    public void setConnectionManager(ConnectionManager cm) {
        this.connManager = cm;
    }
    public void start() {
        logger.info("[{}] ReplicationManager started", localNode.getNodeId());
    }
    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    // Replication
    /**
     * Replicate a message to peers according to the configured consistency model.
     *
     * @return true if the required quorum acknowledged
     */
    public boolean replicate(Message message) {
        List<NodeInfo> targets = selectTargets();
        if (targets.isEmpty()) {
            logger.warn("[{}] No live replicas available for {}",
                    localNode.getNodeId(), message.getMessageId());
            return "EVENTUAL".equalsIgnoreCase(consistencyModel); // allow under
            eventual
        }
        int required = requiredAcks(targets.size());
        AtomicInteger acks = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(required);
        for (NodeInfo target : targets) {
            executor.submit(() -> {
                long start = System.currentTimeMillis();
                if (sendReplicationRpc(target, message)) {
                    PerformanceLogger.logLatency("replication_rpc", start);
                    acks.incrementAndGet();
                    if (acks.get() <= required) latch.countDown();
                }
            });
        }
        try {
            boolean quorumMet = latch.await(Constants.REPLICATION_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS);
            if (!quorumMet) {
                logger.warn("[{}] Replication quorum NOT met for {}: {}/{} acks",
                        localNode.getNodeId(), message.getMessageId(), acks.get(),
                        required);
            }
            return quorumMet;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    private boolean sendReplicationRpc(NodeInfo target, Message message) {
        if (connManager == null) return false;
        try {
            MessageProtocol msg = new MessageProtocol(
                    MessageType.REPLICATE_MESSAGE,
                    localNode.getNodeId(),
                    target.getNodeId(),
                    message
            );
            connManager.sendMessage(target.getNodeId(), msg);
            logger.debug("[{}] Replicated {} to {}",
                    localNode.getNodeId(), message.getMessageId(),
                    target.getNodeId());
            return true;
        } catch (IOException e) {
            logger.warn("[{}] Replication to {} failed: {}",
                    localNode.getNodeId(), target.getNodeId(), e.getMessage());
            return false;
        }
    }
    /** Called on the receiver side when a REPLICATE_MESSAGE arrives. */
    public void handleIncomingReplica(Message message) {
        if (store.storeMessage(message)) {
            logger.debug("[{}] Stored replica {}", localNode.getNodeId(),
                    message.getMessageId());
            PerformanceLogger.increment("messages_replicated_received");
        }
    }
    // ----------------------------------------------------------------
    // Node management
    // ----------------------------------------------------------------
    public void markNodeAlive(String nodeId, boolean alive) {
        NodeInfo n = replicas.get(nodeId);
        if (n != null) n.setAlive(alive);
        quorumManager.markNode(nodeId, alive);
    }
    public boolean hasQuorum() {
        return quorumManager.hasWriteQuorum();
    }
    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------
    private List<NodeInfo> selectTargets() {
        List<NodeInfo> alive = new ArrayList<>();
        for (NodeInfo n : replicas.values()) {
            if (n.isAlive()) alive.add(n);
        }
        Collections.shuffle(alive);
        int count = Math.min(replicationFactor - 1, alive.size());
        return alive.subList(0, count);
    }
    private int requiredAcks(int targetCount) {
        switch (consistencyModel.toUpperCase()) {
            case "STRONG":
                return targetCount;
            case "EVENTUAL":
                return 1;
            case "QUORUM":
            default:
                return (targetCount / 2) + 1;
        }
    }
}
package com.messaging.fault;

import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.consensus.raft.RaftNode;
import com.messaging.network.ConnectionManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.storage.MessageStore;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryManager {
    private static final Logger logger = LoggerUtil.getLogger(RecoveryManager.class);
    private final NodeInfo localNode;
    private final MessageStore store;
    private final RaftNode raft;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private ConnectionManager connManager;

    public RecoveryManager(NodeInfo localNode, MessageStore store, RaftNode raft) {
        this.localNode = localNode;
        this.store = store;
        this.raft = raft;
    }

    public void setConnectionManager(ConnectionManager cm) { this.connManager = cm; }
    public void start() { logger.info("[{}] RecoveryManager started", localNode.getNodeId()); }
    public void shutdown() { executor.shutdown(); }

    public void startLocalRecovery() {
        if (!recovering.compareAndSet(false, true)) {
            logger.warn("Recovery already in progress");
            return;
        }
        executor.submit(() -> {
            try {
                logger.info("[{}] Starting recovery. {} messages on disk", localNode.getNodeId(), store.getMessageCount());
                verifyIntegrity();
                requestMissingMessages();
                logger.info("[{}] Recovery complete", localNode.getNodeId());
            } catch (Exception e) {
                logger.error("Recovery failed: {}", e.getMessage(), e);
            } finally {
                recovering.set(false);
            }
        });
    }

    private void verifyIntegrity() {
        logger.info("[{}] Verifying store integrity ({} messages)", localNode.getNodeId(), store.getMessageCount());
    }

    private void requestMissingMessages() {
        String leaderId = raft.getLeaderId();
        if (leaderId == null || leaderId.equals(localNode.getNodeId())) return;
        if (connManager == null) return;
        long commitIndex = raft.getCommitIndex();
        StateTransferRequest req = new StateTransferRequest(localNode.getNodeId(), commitIndex);
        try {
            connManager.sendMessage(leaderId, new MessageProtocol(MessageType.STATE_TRANSFER_REQUEST, localNode.getNodeId(), leaderId, req));
        } catch (IOException e) {
            logger.warn("Could not request state transfer from {}: {}", leaderId, e.getMessage());
        }
    }

    public void handleStateTransferRequest(MessageProtocol msg) {
        StateTransferRequest req = (StateTransferRequest) msg.getPayload();
        logger.info("[{}] State transfer request from {} (lastIndex={})", localNode.getNodeId(), req.requesterId, req.lastCommitIndex);
        executor.submit(() -> {
            List<Message> allMessages = store.getAllMessages();
            StateTransferResponse resp = new StateTransferResponse(localNode.getNodeId(), allMessages);
            if (connManager == null) return;
            try {
                connManager.sendMessage(req.requesterId, new MessageProtocol(MessageType.STATE_TRANSFER_RESPONSE, localNode.getNodeId(), req.requesterId, resp));
            } catch (IOException e) {
                logger.warn("Failed sending state transfer to {}: {}", req.requesterId, e.getMessage());
            }
        });
    }

    public void handleStateTransferResponse(MessageProtocol msg) {
        StateTransferResponse resp = (StateTransferResponse) msg.getPayload();
        logger.info("[{}] Received {} messages in state transfer from {}", localNode.getNodeId(), resp.messages.size(), resp.senderId);
        resp.messages.forEach(store::storeMessage);
    }

    public static final class StateTransferRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String requesterId;
        public final long lastCommitIndex;
        public StateTransferRequest(String requesterId, long lastCommitIndex) {
            this.requesterId = requesterId;
            this.lastCommitIndex = lastCommitIndex;
        }
    }

    public static final class StateTransferResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String senderId;
        public final List<Message> messages;
        public StateTransferResponse(String senderId, List<Message> messages) {
            this.senderId = senderId;
            this.messages = messages;
        }
    }
}
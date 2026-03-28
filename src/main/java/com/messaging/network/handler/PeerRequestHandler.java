package com.messaging.network.handler;

import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.consensus.raft.RaftNode;
import com.messaging.fault.FailureDetector;
import com.messaging.fault.HeartbeatManager;
import com.messaging.fault.RecoveryManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.replication.ReplicationManager;
import com.messaging.timesync.TimeSyncManager;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

/**
 * Routes inbound MessageProtocol objects (from peers) to the correct component.
 * Registered as the ConnectionManager's messageHandler for peer connections.
 */
public class PeerRequestHandler {
    private static final Logger logger = LoggerUtil.getLogger(PeerRequestHandler.class);

    private final NodeInfo            localNode;
    private final RaftNode            raft;
    private final ReplicationManager  replication;
    private final FailureDetector     failureDetector;
    private final HeartbeatManager    heartbeatManager;
    private final RecoveryManager     recoveryManager;
    private final TimeSyncManager     timeSyncManager;

    public PeerRequestHandler(NodeInfo localNode,
                              RaftNode raft,
                              ReplicationManager replication,
                              FailureDetector failureDetector,
                              HeartbeatManager heartbeatManager,
                              RecoveryManager recoveryManager,
                              TimeSyncManager timeSyncManager) {
        this.localNode        = localNode;
        this.raft             = raft;
        this.replication      = replication;
        this.failureDetector  = failureDetector;
        this.heartbeatManager = heartbeatManager;
        this.recoveryManager  = recoveryManager;
        this.timeSyncManager  = timeSyncManager;
    }

    public void handle(MessageProtocol msg) {
        if (msg == null) return;
        MessageType type = msg.getType();
        logger.debug("[{}] Peer message: {} from {}", localNode.getNodeId(), type, msg.getSourceNodeId());

        switch (type) {

            // Raft consensus
            case REQUEST_VOTE:
            case VOTE_RESPONSE:
            case APPEND_ENTRIES:
            case APPEND_RESPONSE:
                raft.handleIncoming(msg);
                break;

            // Heartbeats
            case HEARTBEAT:
                failureDetector.recordHeartbeat(msg.getSourceNodeId());
                heartbeatManager.sendAck(msg.getSourceNodeId(),
                        ((HeartbeatManager.HeartbeatPayload) msg.getPayload()).sentAtMs);
                break;

            case HEARTBEAT_ACK:
                failureDetector.recordHeartbeat(msg.getSourceNodeId());
                break;

            // Replication
            case REPLICATE_MESSAGE:
                replication.handleIncomingReplica((Message) msg.getPayload());
                break;

            // Time sync
            case TIME_SYNC_REQUEST:
                timeSyncManager.handleRequest(msg);
                break;

            case TIME_SYNC_RESPONSE:
                timeSyncManager.handleResponse(msg);
                break;

            // Recovery
            case STATE_TRANSFER_REQUEST:
                recoveryManager.handleStateTransferRequest(msg);
                break;

            case STATE_TRANSFER_RESPONSE:
                recoveryManager.handleStateTransferResponse(msg);
                break;

            default:
                logger.warn("[{}] PeerRequestHandler: unhandled type {}", localNode.getNodeId(), type);
        }
    }
}

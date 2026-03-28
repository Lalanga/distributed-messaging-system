package com.messaging.server;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.config.ServerConfig;
import com.messaging.consensus.raft.RaftNode;
import com.messaging.fault.*;
import com.messaging.network.ConnectionManager;
import com.messaging.network.handler.ClientRequestHandler;
import com.messaging.network.handler.PeerRequestHandler;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.replication.ReplicationManager;
import com.messaging.service.MessageService;
import com.messaging.storage.MessageStore;
import com.messaging.timesync.TimeSyncManager;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top-level server: constructs, wires, and starts all components.
 *
 * Startup order:
 *   1. MessageStore      — must be ready before any messages arrive
 *   2. ConnectionManager — opens server socket
 *   3. TimeSyncManager   — start syncing before processing messages
 *   4. RaftNode          — consensus
 *   5. ReplicationManager
 *   6. HeartbeatManager + FailureDetector + FailoverManager + RecoveryManager
 *   7. MessageService + handlers — now safe to accept traffic
 */
public class MessagingServer {

    private static final Logger logger = LoggerUtil.getLogger(MessagingServer.class);

    // Client-facing message types (routed to ClientRequestHandler)
    private static final Set<MessageType> CLIENT_TYPES = EnumSet.of(
            MessageType.SEND_MESSAGE,
            MessageType.GET_MESSAGES
    );

    private final ServerConfig config;
    private final NodeInfo localNode;
    private final ServerState serverState;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Components — populated in init()
    private MessageStore store;
    private ConnectionManager connManager;
    private TimeSyncManager timeSync;
    private RaftNode raft;
    private ReplicationManager replication;
    private HeartbeatManager heartbeat;
    private FailureDetector failureDetector;
    private FailoverManager failover;
    private RecoveryManager recovery;
    private MessageService service;
    private ClientRequestHandler clientHandler;
    private PeerRequestHandler peerHandler;

    public MessagingServer(ServerConfig config) {
        this.config = config;
        this.localNode = new NodeInfo(config.getServerId(), "localhost", config.getPort());
        this.serverState = new ServerState(localNode);
    }

    //  Start

    public void start() throws IOException, InterruptedException {
        logger.info("======== Starting server: {} ========", localNode);
        running.set(true);

        initComponents();
        wireComponents();
        startComponents();

        serverState.setReady(true);
        logger.info("======== Server {} ready ========", localNode.getNodeId());

        // Block until shutdown
        while (running.get()) {
            Thread.sleep(500);
        }
    }

    private void initComponents() throws IOException {
        String dataDir = config.getDataDir();

        store = new MessageStore(dataDir, localNode.getNodeId());
        connManager = new ConnectionManager(localNode);
        connManager.registerPeers(config.getPeers());

        timeSync = new TimeSyncManager(localNode, config.getPeers());
        timeSync.setConnectionManager(connManager);

        raft = new RaftNode(localNode, config.getPeers(),
                config.getElectionTimeout(), dataDir);
        raft.setConnectionManager(connManager);

        replication = new ReplicationManager(localNode, store, config.getPeers(),
                config.getReplicationFactor(),
                config.getConsistencyModel());
        replication.setConnectionManager(connManager);

        heartbeat = new HeartbeatManager(localNode, config.getPeers(),
                config.getHeartbeatInterval());
        heartbeat.setConnectionManager(connManager);

        failureDetector = new FailureDetector(localNode, config.getPeers(),
                config.getHeartbeatInterval());
        failover = new FailoverManager(raft, replication);
        recovery = new RecoveryManager(localNode, store, raft);
        recovery.setConnectionManager(connManager);

        service = new MessageService(store, replication, raft, timeSync);

        clientHandler = new ClientRequestHandler(localNode, service);
        clientHandler.setConnectionManager(connManager);

        peerHandler = new PeerRequestHandler(
                localNode, raft, replication,
                failureDetector, heartbeat,
                recovery, timeSync
        );
    }

    private void wireComponents() {
        // FailureDetector calls back into FailoverManager
        failureDetector.setOnFailure(failover::onNodeFailed);
        failureDetector.setOnRecovery(failover::onNodeRecovered);

        // Single dispatch function — routes by message type
        connManager.setMessageHandler(this::dispatch);
    }

    private void startComponents() throws IOException {
        store.start();
        connManager.start();
        timeSync.start();
        raft.start();
        replication.start();
        heartbeat.start();
        failureDetector.start();
        failover.start();
        recovery.start();
    }

    //  Message routing

    private void dispatch(MessageProtocol msg) {
        if (msg == null) return;
        if (CLIENT_TYPES.contains(msg.getType())) {
            clientHandler.handle(msg);
        } else {
            peerHandler.handle(msg);
        }
    }

    //  Shutdown

    public void shutdown() {
        logger.info("Shutting down server: {}", localNode.getNodeId());
        running.set(false);
        serverState.setReady(false);

        // Reverse order of startup
        if (recovery != null) recovery.shutdown();
        if (failover != null) failover.shutdown();
        if (failureDetector != null) failureDetector.shutdown();
        if (heartbeat != null) heartbeat.shutdown();
        if (replication != null) replication.shutdown();
        if (raft != null) raft.shutdown();
        if (timeSync != null) timeSync.shutdown();
        if (connManager != null) connManager.shutdown();
        if (store != null) store.shutdown();

        logger.info("Server {} shutdown complete", localNode.getNodeId());
    }

    public boolean isReady() {
        return serverState.isReady();
    }

    public boolean isLeader() {
        return raft != null && raft.isLeader();
    }
}
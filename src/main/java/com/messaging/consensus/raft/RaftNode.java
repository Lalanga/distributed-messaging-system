package com.messaging.consensus.raft;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.common.enums.NodeRole;
import com.messaging.network.ConnectionManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.util.logger.LoggerUtil;
import com.messaging.util.logger.PerformanceLogger;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {

    private static final Logger logger = LoggerUtil.getLogger(RaftNode.class);

    private final NodeInfo localNode;
    private final List<NodeInfo> peers;
    private final RaftState state;
    private final LeaderElection election;

    private final List<LogEntry> log = new CopyOnWriteArrayList<>();
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    private ConnectionManager connManager;

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(3);

    private volatile boolean running = false;
    private ScheduledFuture<?> electionTimer;

    private volatile PendingElection pendingElection;

    public RaftNode(NodeInfo localNode, List<NodeInfo> peers,
                    int electionTimeoutMs, String dataDir) {
        this.localNode = localNode;
        this.peers = Collections.unmodifiableList(new ArrayList<>(peers));
        this.state = new RaftState(localNode.getNodeId(), electionTimeoutMs, dataDir);
        this.election = new LeaderElection(state, peers);
    }

    public void setConnectionManager(ConnectionManager cm) {
        this.connManager = cm;
    }

    public void start() {
        running = true;
        resetElectionTimer();
        logger.info("[{}] RaftNode started", localNode.getNodeId());
    }

    public void shutdown() {
        running = false;

        if (electionTimer != null) {
            electionTimer.cancel(false);
        }

        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("[{}] RaftNode stopped", localNode.getNodeId());
    }

    public NodeRole getRole() {
        return state.getRole();
    }

    public long getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public String getLeaderId() {
        return state.getLeaderId();
    }

    public boolean isLeader() {
        return state.getRole() == NodeRole.LEADER;
    }

    public long getCommitIndex() {
        return state.getCommitIndex();
    }

    public int getLogSize() {
        return log.size();
    }

    public String getNodeId() {
        return localNode.getNodeId();
    }
}
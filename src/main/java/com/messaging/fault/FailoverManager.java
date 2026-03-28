package com.messaging.fault;

import com.messaging.consensus.raft.RaftNode;
import com.messaging.replication.ReplicationManager;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FailoverManager {
    private static final Logger logger = LoggerUtil.getLogger(FailoverManager.class);

    private final RaftNode raft;
    private final ReplicationManager replication;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public FailoverManager(RaftNode raft, ReplicationManager replication) {
        this.raft = raft;
        this.replication = replication;
    }

    public void start() {
        logger.info("FailoverManager started");
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void onNodeFailed(String nodeId) {
        logger.warn("Handling failover for dead node: {}", nodeId);
        replication.markNodeAlive(nodeId, false);

        executor.submit(() -> {
            if (raft.isLeader()) {
                logger.info("Leader handling failover: redistributing load from {}", nodeId);
            } else {
                logger.info("Follower noted failure of {}; waiting for leader action", nodeId);
            }
        });
    }

    public void onNodeRecovered(String nodeId) {
        logger.info("Node {} recovered, reintegrating", nodeId);
        replication.markNodeAlive(nodeId, true);
    }
}
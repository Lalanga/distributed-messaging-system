package com.messaging.consensus.raft;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.NodeRole;
import com.messaging.util.Constants;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Encapsulates election related helper logic used by RaftNode.
 */
public class LeaderElection {

    private final RaftState state;
    private final List<NodeInfo> peers;

    public LeaderElection(RaftState state, List<NodeInfo> peers) {
        this.state = state;
        this.peers = peers;
    }

    public boolean shouldStartElection() {
        return state.getRole() != NodeRole.LEADER && state.isElectionTimeout();
    }

    /**
     * Adds a small random delay to the base election timeout
     * to avoid multiple nodes starting elections at the same time.
     */
    public long randomisedTimeoutMs() {
        return state.getElectionTimeoutMs()
                + ThreadLocalRandom.current()
                .nextInt(Constants.ELECTION_TIMEOUT_JITTER);
    }

    public boolean hasMajority(int votesReceived) {
        int clusterSize = peers.size() + 1; // peers + self
        return votesReceived > clusterSize / 2;
    }

    public int clusterSize() {
        return peers.size() + 1;
    }
}
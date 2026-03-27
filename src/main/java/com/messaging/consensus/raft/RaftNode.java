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

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    private volatile boolean running = false;

    private ScheduledFuture<?> electionTimer;

    public RaftNode(NodeInfo localNode,
                    List<NodeInfo> peers,
                    int electionTimeoutMs,
                    String dataDir) {

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

    public void handleIncoming(MessageProtocol msg) {
        switch (msg.getType()) {
            case REQUEST_VOTE:
                handleRequestVote(msg);
                break;
            case VOTE_RESPONSE:
                handleVoteResponse(msg);
                break;
            case APPEND_ENTRIES:
                handleAppendEntries(msg);
                break;
            case APPEND_RESPONSE:
                handleAppendResponse(msg);
                break;
            default:
                logger.warn(
                        "[{}] RaftNode received unexpected type {}",
                        localNode.getNodeId(),
                        msg.getType()
                );
        }
    }


    private synchronized void startElection() {
        if (!running) {
            return;
        }

        long term = state.incrementTerm();
        state.setRole(NodeRole.CANDIDATE);
        state.setVotedFor(localNode.getNodeId());

        logger.info("[{}] Starting election for term {}", localNode.getNodeId(), term);
        PerformanceLogger.increment("elections_started");

        AtomicInteger votes = new AtomicInteger(1); // vote for self

        // Single-node cluster: we already have majority - become leader immediately.
        if (peers.isEmpty()) {
            becomeLeader();
            return;
        }

        CountDownLatch latch = new CountDownLatch(peers.size());

        VoteRequest req = new VoteRequest(
                term,
                localNode.getNodeId(),
                lastLogIndex(),
                lastLogTerm()
        );

        for (NodeInfo peer : peers) {
            scheduler.submit(() -> {
                try {
                    sendRequestVote(peer, req);
                } finally {
                    latch.countDown();
                }
            });
        }

        pendingElection = new PendingElection(term, votes, latch, peers.size());
    }

    // RPC- RequestVote (sender side)

    private void sendRequestVote(NodeInfo peer, VoteRequest req) {
        if (connManager == null) {
            return;
        }

        try {
            MessageProtocol msg = new MessageProtocol(
                    MessageType.REQUEST_VOTE,
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    req
            );

            connManager.sendMessage(peer.getNodeId(), msg);
        } catch (IOException e) {
            logger.warn(
                    "[{}] Could not send RequestVote to {}: {}",
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    e.getMessage()
            );
        }
    }

    // RPC- RequestVote (receiver side)

    private void handleRequestVote(MessageProtocol msg) {
        VoteRequest req = (VoteRequest) msg.getPayload();
        boolean granted = false;

        synchronized (this) {
            if (req.term > state.getCurrentTerm()) {
                state.setCurrentTerm(req.term);
                state.setRole(NodeRole.FOLLOWER);
            }

            boolean termOk = req.term >= state.getCurrentTerm();

            boolean notVotedYet =
                    state.getVotedFor() == null ||
                            state.getVotedFor().equals(req.candidateId);

            boolean logUpToDate =
                    req.lastLogTerm > lastLogTerm() ||
                            (req.lastLogTerm == lastLogTerm() &&
                                    req.lastLogIndex >= lastLogIndex());

            if (termOk && notVotedYet && logUpToDate) {
                state.setVotedFor(req.candidateId);
                state.updateLastHeartbeat(); // reset election timer
                granted = true;
            }
        }

        VoteResponse resp = new VoteResponse(state.getCurrentTerm(), granted);

        sendReply(
                msg.getSourceNodeId(),
                MessageType.VOTE_RESPONSE,
                resp
        );

        logger.debug(
                "[{}] Vote for {}: {}",
                localNode.getNodeId(),
                req.candidateId,
                granted
        );
    }

    //RPC- VoteResponse (receiver side)

    private volatile PendingElection pendingElection;

    private void handleVoteResponse(MessageProtocol msg) {
        VoteResponse resp = (VoteResponse) msg.getPayload();

        synchronized (this) {
            if (resp.term > state.getCurrentTerm()) {
                state.setCurrentTerm(resp.term);
                state.setRole(NodeRole.FOLLOWER);
                return;
            }

            if (state.getRole() != NodeRole.CANDIDATE) {
                return;
            }

            if (pendingElection == null) {
                return;
            }

            if (resp.term != state.getCurrentTerm()) {
                return;
            }

            if (resp.granted) {
                int total = pendingElection.votes.incrementAndGet();

                if (election.hasMajority(total)
                        && state.getRole() == NodeRole.CANDIDATE) {
                    becomeLeader();
                }
            }
        }
    }
}
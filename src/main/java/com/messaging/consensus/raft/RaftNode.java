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
        if (electionTimer != null) electionTimer.cancel(false);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS))
                scheduler.shutdownNow();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("[{}] RaftNode stopped", localNode.getNodeId());
    }

    public void handleIncoming(MessageProtocol msg) {
        switch (msg.getType()) {
            case REQUEST_VOTE:   handleRequestVote(msg);   break;
            case VOTE_RESPONSE:  handleVoteResponse(msg);  break;
            case APPEND_ENTRIES: handleAppendEntries(msg); break;
            case APPEND_RESPONSE:handleAppendResponse(msg);break;
            default: logger.warn("[{}] RaftNode received unexpected type {}", localNode.getNodeId(), msg.getType());
        }
    }


    private synchronized void startElection() {
        if (!running) return;
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
                lastLogTerm());

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
        if (connManager == null) return;
        try {
            MessageProtocol msg = new MessageProtocol(
                    MessageType.REQUEST_VOTE,
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    req);
            connManager.sendMessage(peer.getNodeId(), msg);
        } catch (IOException e) {
            logger.warn("[{}] Could not send RequestVote to {}: {}",
                    localNode.getNodeId(), peer.getNodeId(), e.getMessage());
        }
    }

    // RPC - RequestVote (receiver side)

    private void handleRequestVote(MessageProtocol msg) {
        VoteRequest req  = (VoteRequest) msg.getPayload();
        boolean granted  = false;

        synchronized (this) {
            if (req.term > state.getCurrentTerm()) {
                state.setCurrentTerm(req.term);
                state.setRole(NodeRole.FOLLOWER);
            }

            boolean termOk       = req.term >= state.getCurrentTerm();
            boolean notVotedYet  = state.getVotedFor() == null
                    || state.getVotedFor().equals(req.candidateId);
            boolean logUpToDate  = req.lastLogTerm > lastLogTerm()
                    || (req.lastLogTerm == lastLogTerm() && req.lastLogIndex >= lastLogIndex());

            if (termOk && notVotedYet && logUpToDate) {
                state.setVotedFor(req.candidateId);
                state.updateLastHeartbeat(); // reset election timer
                granted = true;
            }
        }

        VoteResponse resp = new VoteResponse(state.getCurrentTerm(), granted);
        sendReply(msg.getSourceNodeId(), MessageType.VOTE_RESPONSE, resp);
        logger.debug("[{}] Vote for {}: {}", localNode.getNodeId(), req.candidateId, granted);
    }

    // RPC - VoteResponse (receiver side)

    private volatile PendingElection pendingElection;

    private void handleVoteResponse(MessageProtocol msg) {
        VoteResponse resp = (VoteResponse) msg.getPayload();

        synchronized (this) {
            if (resp.term > state.getCurrentTerm()) {
                state.setCurrentTerm(resp.term);
                state.setRole(NodeRole.FOLLOWER);
                return;
            }

            if (state.getRole() != NodeRole.CANDIDATE) return;
            if (pendingElection == null) return;
            if (resp.term != state.getCurrentTerm()) return;

            if (resp.granted) {
                int total = pendingElection.votes.incrementAndGet();
                if (election.hasMajority(total) && state.getRole() == NodeRole.CANDIDATE) {
                    becomeLeader();
                }
            }
        }
    }

    public long appendCommand(byte[] data) {
        synchronized (this) {
            if (state.getRole() != NodeRole.LEADER) return -1L;

            LogEntry entry = new LogEntry(
                    lastLogIndex() + 1,
                    state.getCurrentTerm(),
                    LogEntry.EntryType.MESSAGE,
                    data);
            log.add(entry);
            logger.debug("[{}] Appended entry index={}", localNode.getNodeId(), entry.getIndex());
            replicateToAll();
            return entry.getIndex();
        }
    }

    private void replicateToAll() {
        for (NodeInfo peer : peers) {
            scheduler.submit(() -> sendAppendEntries(peer));
        }
    }

    // RPC - AppendEntries (sender side - also used as heartbeat)

    private void sendAppendEntries(NodeInfo peer) {
        if (connManager == null || state.getRole() != NodeRole.LEADER) return;

        long ni         = nextIndex.getOrDefault(peer.getNodeId(), lastLogIndex() + 1);
        long prevIdx    = ni - 1;
        long prevTerm   = prevIdx > 0 && prevIdx <= log.size()
                ? log.get((int) prevIdx - 1).getTerm() : 0;

        List<LogEntry> entries = new ArrayList<>();
        for (long i = ni; i <= log.size(); i++) {
            entries.add(log.get((int) i - 1));
        }

        AppendEntriesRequest req = new AppendEntriesRequest(
                state.getCurrentTerm(),
                localNode.getNodeId(),
                prevIdx, prevTerm,
                entries,
                state.getCommitIndex());

        try {
            MessageProtocol msg = new MessageProtocol(
                    MessageType.APPEND_ENTRIES,
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    req);
            connManager.sendMessage(peer.getNodeId(), msg);
        } catch (IOException e) {
            logger.warn("[{}] AppendEntries to {} failed: {}",
                    localNode.getNodeId(), peer.getNodeId(), e.getMessage());
        }
    }

    // RPC - AppendEntries (receiver side)

    private void handleAppendEntries(MessageProtocol msg) {
        AppendEntriesRequest req = (AppendEntriesRequest) msg.getPayload();
        boolean success = false;

        synchronized (this) {
            if (req.term > state.getCurrentTerm()) {
                state.setCurrentTerm(req.term);
                state.setRole(NodeRole.FOLLOWER);
            }

            if (req.term < state.getCurrentTerm()) {
            } else {
                state.setLeaderId(req.leaderId);
                state.updateLastHeartbeat(); // valid heartbeat, reset election timer

                if (state.getRole() == NodeRole.CANDIDATE)
                    state.setRole(NodeRole.FOLLOWER);

                boolean prevMatch = req.prevLogIndex == 0
                        || (req.prevLogIndex <= log.size()
                        && log.get((int) req.prevLogIndex - 1).getTerm() == req.prevLogTerm);

                if (prevMatch) {
                    long insertAt = req.prevLogIndex;
                    for (LogEntry entry : req.entries) {
                        long idx = entry.getIndex();
                        if (idx <= log.size()) {
                            if (log.get((int) idx - 1).getTerm() != entry.getTerm()) {
                                while (log.size() >= idx) log.remove(log.size() - 1);
                                log.add(entry);
                            }
                        } else {
                            log.add(entry);
                        }
                    }

                    // Advance commit index
                    if (req.leaderCommit > state.getCommitIndex()) {
                        state.setCommitIndex(Math.min(req.leaderCommit, lastLogIndex()));
                    }
                    success = true;
                }
            }
        }

        AppendEntriesResponse resp = new AppendEntriesResponse(
                state.getCurrentTerm(), success, lastLogIndex());
        sendReply(msg.getSourceNodeId(), MessageType.APPEND_RESPONSE, resp);
    }

    // RPC - AppendResponse (leader receiver)

    private void handleAppendResponse(MessageProtocol msg) {
        AppendEntriesResponse resp = (AppendEntriesResponse) msg.getPayload();
        String peerId = msg.getSourceNodeId();

        synchronized (this) {
            if (resp.term > state.getCurrentTerm()) {
                state.setCurrentTerm(resp.term);
                state.setRole(NodeRole.FOLLOWER);
                return;
            }

            if (state.getRole() != NodeRole.LEADER) return;

            if (resp.success) {
                nextIndex.put(peerId, resp.matchIndex + 1);
                matchIndex.put(peerId, resp.matchIndex);
                advanceCommitIndex();
            } else {
                long ni = nextIndex.getOrDefault(peerId, 1L);
                nextIndex.put(peerId, Math.max(1, ni - 1));
                NodeInfo peer = peers.stream()
                        .filter(p -> p.getNodeId().equals(peerId))
                        .findFirst().orElse(null);
                if (peer != null) scheduler.submit(() -> sendAppendEntries(peer));
            }
        }
    }

    private void becomeLeader() {
        state.setRole(NodeRole.LEADER);
        state.setLeaderId(localNode.getNodeId());
        localNode.setRole(NodeRole.LEADER);

        long next = lastLogIndex() + 1;
        peers.forEach(p -> {
            nextIndex.put(p.getNodeId(), next);
            matchIndex.put(p.getNodeId(), 0L);
        });

        LogEntry noop = new LogEntry(next, state.getCurrentTerm(),
                LogEntry.EntryType.NOOP, new byte[0]);
        log.add(noop);

        logger.info("[{}] Became LEADER for term {}", localNode.getNodeId(), state.getCurrentTerm());
        PerformanceLogger.increment("leader_elections_won");

        scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0, state.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        if (!running || state.getRole() != NodeRole.LEADER) return;
        for (NodeInfo peer : peers) {
            scheduler.submit(() -> sendAppendEntries(peer));
        }
    }

    private void advanceCommitIndex() {
        // Find highest N such that a majority have matchIndex >= N and log[N].term == currentTerm
        for (long n = lastLogIndex(); n > state.getCommitIndex(); n--) {
            if (n < 1 || n > log.size()) continue;
            if (log.get((int) n - 1).getTerm() != state.getCurrentTerm()) continue;

            int count = 1; // self
            for (long mi : matchIndex.values()) {
                if (mi >= n) count++;
            }
            if (election.hasMajority(count)) {
                state.setCommitIndex(n);
                logger.debug("[{}] Commit index advanced to {}", localNode.getNodeId(), n);
                break;
            }
        }
    }

    private void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        electionTimer = scheduler.schedule(
                this::electionTick,
                election.randomisedTimeoutMs(),
                TimeUnit.MILLISECONDS);
    }

    private void electionTick() {
        if (!running) return;
        if (state.getRole() != NodeRole.LEADER && state.isElectionTimeout()) {
            startElection();
        }
        resetElectionTimer();
    }


    private long lastLogIndex() { return log.isEmpty() ? 0 : log.get(log.size() - 1).getIndex(); }
    private long lastLogTerm()  { return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm(); }

    private void sendReply(String targetId, MessageType type, Serializable payload) {
        if (connManager == null) return;
        connManager.sendMessageAsync(targetId,
                new MessageProtocol(type, localNode.getNodeId(), targetId, payload));
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

    public static final class VoteRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        final long   term;
        final String candidateId;
        final long   lastLogIndex;
        final long lastLogTerm;

        VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    public static final class VoteResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        final long term; final boolean granted;
        VoteResponse(long term, boolean granted) { this.term = term; this.granted = granted; }
    }

    public static final class AppendEntriesRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        final long term; final String leaderId;
        final long prevLogIndex; final long prevLogTerm;
        final List<LogEntry> entries;
        final long leaderCommit;
        AppendEntriesRequest(long term, String leaderId,
                             long prevLogIndex, long prevLogTerm,
                             List<LogEntry> entries, long leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
    }

    public static final class AppendEntriesResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        final long term;
        final boolean success;
        final long matchIndex;
        AppendEntriesResponse(long term, boolean success, long matchIndex) {
            this.term = term;
            this.success = success;
            this.matchIndex = matchIndex;
        }
    }

    private static final class PendingElection {
        final long term;
        final AtomicInteger votes;
        final CountDownLatch latch;
        final int peerCount;
        PendingElection(long term, AtomicInteger votes, CountDownLatch latch, int peerCount) {
            this.term = term;
            this.votes = votes;
            this.latch = latch;
            this.peerCount = peerCount;
        }
    }
}
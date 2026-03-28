package com.messaging.timesync;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.network.ConnectionManager;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.util.Constants;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
public class TimeSyncManager {
    private static final Logger logger =
            LoggerUtil.getLogger(TimeSyncManager.class);
    private final NodeInfo localNode;
    private final List<NodeInfo> peers;
    private final MessageReorderer reorderer = new MessageReorderer();
    private final TimestampCorrector corrector = new TimestampCorrector();
    private ConnectionManager connManager;
    private volatile long offset = 0L;
    private final Map<String, Long> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "time-sync");
                t.setDaemon(true);
                return t;
            });
    public TimeSyncManager(NodeInfo localNode, List<NodeInfo> peers) {
        this.localNode = localNode;
        this.peers = peers;
    }
    public void setConnectionManager(ConnectionManager cm) {
        this.connManager = cm;
    }
    public void start() {
        scheduler.scheduleAtFixedRate(
                this::syncRound,
                5,
                Constants.TIME_SYNC_INTERVAL_S,
                TimeUnit.SECONDS
        );
        logger.info("[{}] TimeSyncManager started", localNode.getNodeId());
    }
    public void shutdown() {
        scheduler.shutdown();
    }
    private void syncRound() {
        if (connManager == null || peers.isEmpty()) {
            return;
        }
        NodeInfo peer = peers.get(new Random().nextInt(peers.size()));
        try {
            String corrId = UUID.randomUUID().toString();
            long t1 = System.currentTimeMillis();
            pendingRequests.put(corrId, t1);
            TimeSyncPayload req = new TimeSyncPayload(corrId, t1, 0L, false);
            connManager.sendMessage(
                    peer.getNodeId(),
                    new MessageProtocol(
                            MessageType.TIME_SYNC_REQUEST,
                            localNode.getNodeId(),
                            peer.getNodeId(),
                            req
                    )
            );
        } catch (IOException e) {
            logger.debug(
                    "[{}] Time sync request to {} failed: {}",
                    localNode.getNodeId(),
                    peer.getNodeId(),
                    e.getMessage()
            );
        }
    }
    public void handleRequest(MessageProtocol msg) {
        TimeSyncPayload req = (TimeSyncPayload) msg.getPayload();
        long t2 = System.currentTimeMillis();
        long t3 = System.currentTimeMillis();
        TimeSyncPayload resp =
                new TimeSyncPayload(req.correlationId, req.t1, t2, t3, true);
        if (connManager == null) {
            return;
        }
        connManager.sendMessageAsync(
                msg.getSourceNodeId(),
                new MessageProtocol(
                        MessageType.TIME_SYNC_RESPONSE,
                        localNode.getNodeId(),
                        msg.getSourceNodeId(),
                        resp
                )
        );
    }
    public void handleResponse(MessageProtocol msg) {
        TimeSyncPayload resp = (TimeSyncPayload) msg.getPayload();
        Long t1 = pendingRequests.remove(resp.correlationId);
        if (t1 == null) {
            return;
        }
        long t4 = System.currentTimeMillis();
        long sample = ((resp.t2 - t1) + (resp.t3 - t4)) / 2;
        offset = (offset * 3 + sample) / 4;
        corrector.updateOffset(msg.getSourceNodeId(), sample);
        logger.debug(
                "[{}] Time sync offset={}ms (sample={}ms)",
                localNode.getNodeId(),
                offset,
                sample
        );
    }
    public long getSynchronizedTime() {
        return System.currentTimeMillis() + offset;
    }
    public long getOffset() {
        return offset;
    }
    public MessageReorderer getReorderer() {
        return reorderer;
    }
    public TimestampCorrector getCorrector() {
        return corrector;
    }
    public static final class TimeSyncPayload implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String correlationId;
        public final long t1;
        public final long t2;
        public final long t3;
        public final boolean isResponse;
        TimeSyncPayload(String id, long t1, long t2, boolean isResponse) {
            this(id, t1, t2, 0L, isResponse);
        }
        TimeSyncPayload(String id, long t1, long t2, long t3, boolean isResponse) {
            this.correlationId = id;
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
            this.isResponse = isResponse;
        }
    }
}
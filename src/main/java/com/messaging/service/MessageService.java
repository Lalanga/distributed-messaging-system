package com.messaging.service;

import com.messaging.common.Message;
import com.messaging.consensus.raft.RaftNode;
import com.messaging.replication.ReplicationManager;
import com.messaging.storage.MessageStore;
import com.messaging.timesync.TimeSyncManager;
import com.messaging.util.logger.LoggerUtil;
import com.messaging.util.logger.PerformanceLogger;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MessageService {
    private static final Logger logger = LoggerUtil.getLogger(MessageService.class);

    private final MessageStore store;
    private final ReplicationManager replication;
    private final RaftNode raft;
    private final TimeSyncManager timeSync;

    public MessageService(MessageStore store,
                          ReplicationManager replication,
                          RaftNode raft,
                          TimeSyncManager timeSync) {
        this.store = store;
        this.replication = replication;
        this.raft = raft;
        this.timeSync = timeSync;
    }

    public boolean send(Message message) {
        long start = System.currentTimeMillis();

        // 1. Timestamp correction
        long serverTime = timeSync.getSynchronizedTime();
        timeSync.getCorrector().correct(message, serverTime);

        // 2. Enqueue in reorderer so messages arrive in order
        timeSync.getReorderer().enqueue(message);

        // 3. Store locally
        boolean stored = store.storeMessage(message);
        if (!stored) {
            logger.warn("Message {} rejected (duplicate or store error)", message.getMessageId());
            return false;
        }

        // 4. Submit to Raft log (serialise to bytes via toString for simplicity)
        byte[] logData = message.toString().getBytes();
        long logIndex = raft.appendCommand(logData);
        if (logIndex < 0) {
            logger.warn("Not leader; cannot commit message {}", message.getMessageId());
            // Still return true; message is stored locally, replication will catch up
        }

        // 5. Replicate to peers
        boolean quorumMet = replication.replicate(message);
        if (!quorumMet) {
            logger.warn("Replication quorum not met for {}", message.getMessageId());
        }

        PerformanceLogger.logLatency("send_message", start);
        PerformanceLogger.increment("messages_sent");
        logger.info("Message {} stored and replicated (quorum={})", message.getMessageId(), quorumMet);

        return true;
    }

    public List<Message> getMessages(String recipientId, int limit) {
        long start = System.currentTimeMillis();

        // Drain ordered messages from the reorder buffer
        List<Message> ordered = timeSync.getReorderer().drain(recipientId, timeSync.getSynchronizedTime());

        // Persist any newly-ordered messages
        ordered.forEach(store::storeMessage);

        List<Message> result = store.getMessagesForRecipient(recipientId, limit);

        PerformanceLogger.logLatency("get_messages", start);
        return result;
    }
}
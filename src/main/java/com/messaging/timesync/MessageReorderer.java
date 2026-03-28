package com.messaging.timesync;
import com.messaging.common.Message;
import com.messaging.util.Constants;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
/**
 * Buffers out-of-order messages and releases them in timestamp order
 * once the reorder window has elapsed.
 */
public class MessageReorderer {
    // recipientId -> priority queue ordered by serverTimestamp ASC
    private final Map<String, PriorityBlockingQueue<Message>> pending =
            new ConcurrentHashMap<>();
    private final Map<String, Long> lastDeliveredTs =
            new ConcurrentHashMap<>();
    public void enqueue(Message message) {
        pending.computeIfAbsent(
                        message.getRecipientId(),
                        k -> new PriorityBlockingQueue<>(
                                16,

                                Comparator.comparingLong(Message::getServerTimestamp)
                        )
                )
                .offer(message);
    }
    /**
     * Returns messages for {@code recipientId} that are ready to deliver:
     * their server timestamp is at least {@code windowMs} in the past
     * (giving stragglers time to arrive), and they are ordered ascending.
     */
    public List<Message> drain(String recipientId, long nowMs) {
        PriorityBlockingQueue<Message> queue = pending.get(recipientId);
        if (queue == null || queue.isEmpty()) {
            return Collections.emptyList();
        }
        long cutoff = nowMs - Constants.REORDER_WINDOW_MS;
        long lastTs = lastDeliveredTs.getOrDefault(recipientId, 0L);
        List<Message> ready = new ArrayList<>();
        while (!queue.isEmpty()) {
            Message head = queue.peek();
            if (head.getServerTimestamp() <= cutoff
                    && head.getServerTimestamp() >= lastTs) {
                ready.add(queue.poll());
                lastTs = head.getServerTimestamp();
            } else {
                break;
            }
        }
        if (!ready.isEmpty()) {
            lastDeliveredTs.put(recipientId, lastTs);
        }
        return ready;
    }
    public int pendingCount(String recipientId) {
        PriorityBlockingQueue<Message> q = pending.get(recipientId);
        return q == null ? 0 : q.size();
    }
    public void clear(String recipientId) {
        pending.remove(recipientId);
        lastDeliveredTs.remove(recipientId);
    }
}

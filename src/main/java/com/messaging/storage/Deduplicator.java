package com.messaging.storage;
import com.messaging.util.Constants;
import java.util.Map;
import java.util.concurrent.*;
public class Deduplicator {
    private final Map<String, Long> seen = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dedup-cleanup");
                t.setDaemon(true);
                return t;
            });
    private static final long RETENTION_MS =
            Constants.DEDUP_RETENTION_HOURS * 3_600_000L;
    public Deduplicator() {
        scheduler.scheduleAtFixedRate(
                this::evict,
                Constants.DEDUP_RETENTION_HOURS,
                Constants.DEDUP_RETENTION_HOURS,
                TimeUnit.HOURS
        );
    }
    /** Returns true if the id has been seen before (duplicate). */
    public boolean isDuplicate(String id) {
        return seen.containsKey(id);
    }
    /** Record that this id has been processed. */
    public void markProcessed(String id) {
        seen.put(id, System.currentTimeMillis());
    }
    private void evict() {
        long cutoff = System.currentTimeMillis() - RETENTION_MS;
        seen.entrySet().removeIf(e -> e.getValue() < cutoff);
    }
    public void shutdown() {
        scheduler.shutdown();
    }
}
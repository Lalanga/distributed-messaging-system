package com.messaging.util.logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceLogger {
    private static final Logger logger = LogManager.getLogger("Performance");
    private static final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    private PerformanceLogger() {}

    public static void logLatency(String operation, long startTimeMs) {
        long latency = System.currentTimeMillis() - startTimeMs;
        logger.info("LATENCY operation={} latency_ms={}", operation, latency);
    }

    public static void logThroughput(String operation, long count, long periodMs) {
        if (periodMs > 0) {
            double throughput = (count * 1000.0) / periodMs;
            logger.info("THROUGHPUT operation={} ops_per_sec={:.2f}", operation, throughput);
        }
    }

    public static void increment(String counter) {
        counters.computeIfAbsent(counter, k -> new AtomicLong(0)).incrementAndGet();
    }

    public static long getCount(String counter) {
        AtomicLong c = counters.get(counter);
        return c == null ? 0 : c.get();
    }

    public static void reset(String counter) {
        counters.computeIfAbsent(counter, k -> new AtomicLong(0)).set(0);
    }
}

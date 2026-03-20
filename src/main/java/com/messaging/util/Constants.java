package com.messaging.util;

public final class Constants {
    private Constants() {}

    // Network
    public static final int DEFAULT_PORT           = 5000;
    public static final int CONNECTION_TIMEOUT_MS  = 3000;
    public static final int READ_TIMEOUT_MS        = 5000;
    public static final int MAX_RETRIES            = 3;

    // Raft timing
    public static final int HEARTBEAT_INTERVAL_MS  = 500;
    public static final int ELECTION_TIMEOUT_BASE  = 1500;
    public static final int ELECTION_TIMEOUT_JITTER = 500;

    // Replication
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    public static final int REPLICATION_TIMEOUT_MS     = 5000;

    // Time sync
    public static final long MAX_CLOCK_SKEW_MS    = 5000;
    public static final int  TIME_SYNC_INTERVAL_S = 60;
    public static final int  REORDER_WINDOW_MS    = 1000;

    // Storage
    public static final int  DEDUP_RETENTION_HOURS = 1;
    public static final int  MAX_MESSAGES_PER_QUERY = 100;

    // Failure detection
    public static final int  MISSED_HEARTBEATS_THRESHOLD = 3;
}

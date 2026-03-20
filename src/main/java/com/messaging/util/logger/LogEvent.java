package com.messaging.util.logger;

import java.time.Instant;

public class LogEvent {
    private final String nodeId;
    private final String category;
    private final String message;
    private final long timestamp;

    public LogEvent(String nodeId, String category, String message) {
        this.nodeId    = nodeId;
        this.category  = category;
        this.message   = message;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public String getNodeId()    { return nodeId; }
    public String getCategory()  { return category; }
    public String getMessage()   { return message; }
    public long   getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("[%d] [%s] [%s] %s", timestamp, nodeId, category, message);
    }
}

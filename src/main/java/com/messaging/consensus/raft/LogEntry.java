package com.messaging.consensus.raft;

import java.io.Serializable;

public class LogEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum EntryType {
        NOOP,
        MESSAGE,
        CONFIG
    }

    private final long index;
    private final long term;
    private final EntryType type;
    private final byte[] data;       // serialized payload
    private final long timestamp;

    public LogEntry(long index, long term, EntryType type, byte[] data) {
        this.index = index;
        this.term = term;
        this.type = type;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public EntryType getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{index=%d, term=%d, type=%s}", index, term, type);
    }
}
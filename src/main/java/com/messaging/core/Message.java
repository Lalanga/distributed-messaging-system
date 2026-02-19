package com.messaging.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String senderId;
    private final String receiverId;
    private final String content;
    private final Instant clientTimestamp;
    private volatile Instant serverTimestamp;
    private volatile long consensusIndex;
    private volatile int version;
    private volatile boolean isDelivered;
    private volatile String serverId; // Server that processed this message

    public Message(String senderId, String receiverId, String content) {
        this.id = UUID.randomUUID().toString();
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.content = content;
        this.clientTimestamp = Instant.now();
        this.version = 1;
        this.isDelivered = false;
        this.consensusIndex = -1;
    }

    // Getters and Setters
    public String getId() { return id; }
    public String getSenderId() { return senderId; }
    public String getReceiverId() { return receiverId; }
    public String getContent() { return content; }
    public Instant getClientTimestamp() { return clientTimestamp; }
    public Instant getServerTimestamp() { return serverTimestamp; }
    public void setServerTimestamp(Instant serverTimestamp) {
        this.serverTimestamp = serverTimestamp;
    }
    public long getConsensusIndex() { return consensusIndex; }
    public void setConsensusIndex(long consensusIndex) {
        this.consensusIndex = consensusIndex;
    }
    public int getVersion() { return version; }
    public void incrementVersion() { this.version++; }
    public boolean isDelivered() { return isDelivered; }
    public void setDelivered(boolean delivered) { isDelivered = delivered; }
    public String getServerId() { return serverId; }
    public void setServerId(String serverId) { this.serverId = serverId; }

    @Override
    public String toString() {
        return String.format("Message[id=%s, from=%s, to=%s, time=%s, content=%s]",
                id, senderId, receiverId, clientTimestamp, content);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Message message = (Message) obj;
        return id.equals(message.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
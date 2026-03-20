package com.messaging.common;

import java.io.Serializable;
import java.util.UUID;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String messageId;
    private final String content;
    private final String senderId;
    private final String recipientId;
    private final long clientTimestamp;
    private volatile long serverTimestamp;
    private volatile long consensusTimestamp;
    private volatile boolean delivered;

    public Message(String content, String senderId, String recipientId) {
        this.messageId = UUID.randomUUID().toString();
        this.content = content;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.clientTimestamp = System.currentTimeMillis();
        this.serverTimestamp = 0L;
        this.consensusTimestamp = 0L;
        this.delivered = false;
    }

    public String getMessageId() { return messageId; }
    public String getContent() { return content; }
    public String getSenderId() { return senderId; }
    public String getRecipientId() { return recipientId; }
    public long getClientTimestamp() { return clientTimestamp; }
    public long getServerTimestamp() { return serverTimestamp; }
    public void setServerTimestamp(long serverTimestamp) { this.serverTimestamp = serverTimestamp; }
    public long getConsensusTimestamp() { return consensusTimestamp; }
    public void setConsensusTimestamp(long consensusTimestamp) { this.consensusTimestamp = consensusTimestamp; }
    public boolean isDelivered() { return delivered; }
    public void setDelivered(boolean delivered) { this.delivered = delivered; }

    @Override
    public String toString() {
        return String.format("Message{id='%s', from='%s', to='%s', ts=%d}",
                messageId, senderId, recipientId, clientTimestamp);
    }
}
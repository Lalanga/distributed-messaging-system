package com.messaging.network.protocol;

import com.messaging.common.enums.MessageType;

import java.io.Serializable;
import java.util.UUID;

public class MessageProtocol implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String protocolId;
    private final MessageType type;
    private final String sourceNodeId;
    private final String targetNodeId;
    private final Object payload; // must be Serializable
    private final long timestamp;

    public MessageProtocol(MessageType type,
                           String sourceNodeId,
                           String targetNodeId,
                           Object payload) {
        this.protocolId = UUID.randomUUID().toString();
        this.type = type;
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public String getProtocolId() {
        return protocolId;
    }

    public MessageType getType() {
        return type;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public Object getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format(
                "Protocol{id='%s', type=%s, from='%s', to='%s'}",
                protocolId,
                type,
                sourceNodeId,
                targetNodeId
        );
    }
}
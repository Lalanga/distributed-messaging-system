package com.messaging.network;

import com.messaging.common.enums.MessageType;

import java.io.Serializable;

public class Request implements Serializable {

    private static final long serialVersionUID = 1L;

    private final MessageType type;
    private final String requesterId;
    private final Object data;

    public Request(MessageType type, String requesterId, Object data) {
        this.type = type;
        this.requesterId = requesterId;
        this.data = data;
    }

    public MessageType getType() {
        return type;
    }

    public String getRequesterId() {
        return requesterId;
    }

    public Object getData() {
        return data;
    }
}
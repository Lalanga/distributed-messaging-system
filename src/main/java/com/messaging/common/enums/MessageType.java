package com.messaging.common.enums;

public enum MessageType {
    // Client-facing
    SEND_MESSAGE,
    GET_MESSAGES,
    SEND_ACK,
    GET_RESPONSE,

    // Replication
    REPLICATE_MESSAGE,
    REPLICATION_ACK,

    // Raft consensus
    REQUEST_VOTE,
    VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_RESPONSE,

    // Fault tolerance
    HEARTBEAT,
    HEARTBEAT_ACK,
    FAILURE_NOTIFICATION,

    // Time sync
    TIME_SYNC_REQUEST,
    TIME_SYNC_RESPONSE,

    // Recovery
    STATE_TRANSFER_REQUEST,
    STATE_TRANSFER_RESPONSE,
    STATE_TRANSFER_COMPLETE,

    // Error
    ERROR
}

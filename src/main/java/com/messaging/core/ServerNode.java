package com.messaging.core;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ServerNode {
    CompletableFuture<Boolean> sendMessage(Message message);
    CompletableFuture<List<Message>> getMessages(String receiverId, Instant from, Instant to);
    CompletableFuture<Boolean> replicateMessage(Message message, String fromServer);
    CompletableFuture<Boolean> handleHeartbeat(String fromServer);
    NodeRole getRole();
    void setRole(NodeRole role);
    String getServerId();
    void start();
    void stop();
    boolean isHealthy();
    ServerConfig getConfig();
}
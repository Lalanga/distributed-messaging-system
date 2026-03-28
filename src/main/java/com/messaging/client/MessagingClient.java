package com.messaging.client;

import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.network.ConnectionManager;
import com.messaging.network.Response;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class MessagingClient {

    private static final Logger logger = LoggerUtil.getLogger(MessagingClient.class);
    private static final int RESPONSE_TIMEOUT_MS = 5000;

    private final String clientId;
    private ConnectionManager connManager;
    private NodeInfo server;

    // Pending RPC responses: correlationId -> CompletableFuture
    private final Map<String, CompletableFuture<Response>> pending = new ConcurrentHashMap<>();

    public MessagingClient() {
        this.clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String getClientId() {
        return clientId;
    }

    public void connect(String host, int port) throws IOException {
        server = new NodeInfo("server", host, port);
        NodeInfo me = new NodeInfo(clientId, "localhost", 0);
        connManager = new ConnectionManager(me);
        connManager.registerPeer(server);
        connManager.setMessageHandler(this::handleResponse);
        connManager.start();
        logger.info("Client {} connected to {}:{}", clientId, host, port);
    }

    public void disconnect() {
        if (connManager != null) {
            connManager.shutdown();
        }
    }

    // API

    public boolean sendMessage(String recipientId, String content) throws IOException {
        Message msg = new Message(content, clientId, recipientId);
        String corrId = msg.getMessageId();
        CompletableFuture<Response> future = new CompletableFuture<>();
        pending.put(corrId, future);

        MessageProtocol protocol = new MessageProtocol(
                MessageType.SEND_MESSAGE, clientId, "server", msg
        );
        connManager.sendMessage("server", protocol);

        try {
            Response resp = future.get(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return resp.isSuccess();
        } catch (TimeoutException e) {
            logger.warn("sendMessage timed out for {}", corrId);
            return false;
        } catch (Exception e) {
            throw new IOException("sendMessage failed: " + e.getMessage(), e);
        } finally {
            pending.remove(corrId);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Message> getMessages(int limit) throws IOException {
        String corrId = UUID.randomUUID().toString();
        CompletableFuture<Response> future = new CompletableFuture<>();
        pending.put(corrId, future);

        Object[] payload = {clientId, limit};
        MessageProtocol protocol = new MessageProtocol(
                MessageType.GET_MESSAGES, clientId, "server", payload
        );
        connManager.sendMessage("server", protocol);

        try {
            Response resp = future.get(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (resp.isSuccess()) {
                return (List<Message>) resp.getData();
            }
            logger.warn("getMessages returned error: {}", resp.getErrorMessage());
            return Collections.emptyList();
        } catch (TimeoutException e) {
            logger.warn("getMessages timed out");
            return Collections.emptyList();
        } catch (Exception e) {
            throw new IOException("getMessages failed: " + e.getMessage(), e);
        } finally {
            pending.remove(corrId);
        }
    }

    // Response handler

    private void handleResponse(MessageProtocol msg) {
        if (msg.getPayload() instanceof Response) {
            Response resp = (Response) msg.getPayload();
            // Match to the most recent pending future (simple strategy)
            pending.values().stream()
                    .filter(f -> !f.isDone())
                    .findFirst()
                    .ifPresent(f -> f.complete(resp));
        }
    }
}
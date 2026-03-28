package com.messaging.network.handler;

import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.MessageType;
import com.messaging.network.ConnectionManager;
import com.messaging.network.Response;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.service.MessageService;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Routes inbound MessageProtocol objects from clients to MessageService,
 * then sends back a response on the same connection.
 */
public class ClientRequestHandler {
    private static final Logger logger = LoggerUtil.getLogger(ClientRequestHandler.class);

    private final NodeInfo localNode;
    private final MessageService service;
    private ConnectionManager connManager;

    public ClientRequestHandler(NodeInfo localNode, MessageService service) {
        this.localNode = localNode;
        this.service = service;
    }

    public void setConnectionManager(ConnectionManager cm) { this.connManager = cm; }

    public void handle(MessageProtocol msg) {
        if (msg == null) return;
        logger.debug("[{}] Client request: {} from {}", localNode.getNodeId(),
                msg.getType(), msg.getSourceNodeId());

        switch (msg.getType()) {
            case SEND_MESSAGE:
                handleSend(msg);
                break;
            case GET_MESSAGES:
                handleGet(msg);
                break;
            default:
                sendError(msg.getSourceNodeId(), "Unsupported operation: " + msg.getType());
        }
    }

    private void handleSend(MessageProtocol msg) {
        try {
            Message m = (Message) msg.getPayload();
            boolean ok = service.send(m);
            sendResponse(msg.getSourceNodeId(), MessageType.SEND_ACK,
                    ok ? Response.ok(m.getMessageId())
                            : Response.error("Server could not store message"));
        } catch (Exception e) {
            logger.error("Error handling SEND_MESSAGE: {}", e.getMessage(), e);
            sendError(msg.getSourceNodeId(), e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void handleGet(MessageProtocol msg) {
        try {
            // payload is a String recipientId or an Object[] {recipientId, limit}
            Object payload = msg.getPayload();
            String recipientId;
            int    limit = 20;
            if (payload instanceof Object[]) {
                Object[] arr = (Object[]) payload;
                recipientId = (String) arr[0];
                limit = (Integer) arr[1];
            } else {
                recipientId = (String) payload;
            }

            List<Message> messages = service.getMessages(recipientId, limit);
            sendResponse(msg.getSourceNodeId(), MessageType.GET_RESPONSE,
                    Response.ok(messages));
        } catch (Exception e) {
            logger.error("Error handling GET_MESSAGES: {}", e.getMessage(), e);
            sendError(msg.getSourceNodeId(), e.getMessage());
        }
    }

    private void sendResponse(String target, MessageType type, Response resp) {
        if (connManager == null) return;
        connManager.sendMessageAsync(target,
                new MessageProtocol(type, localNode.getNodeId(), target, resp));
    }

    private void sendError(String target, String message) {
        sendResponse(target, MessageType.ERROR, Response.error(message));
    }
}

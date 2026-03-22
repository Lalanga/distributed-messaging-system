package com.messaging.network;

import com.messaging.common.NodeInfo;
import com.messaging.network.protocol.MessageProtocol;
import com.messaging.util.Constants;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Handles all TCP connections in the system, both incoming and outgoing.
 * Incoming connections are accepted through a server socket, while outgoing
 * ones are created only when needed. Received messages are passed to a
 * handler so other parts of the system don’t need to deal with networking details.
 */

public class ConnectionManager {
    private static final Logger logger = LoggerUtil.getLogger(ConnectionManager.class);

    private final NodeInfo localNode;
    private final Map<String, NodeConnection> connections = new ConcurrentHashMap<>();
    private final Map<String, NodeInfo> knownPeers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private Consumer<MessageProtocol> messageHandler;
    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public ConnectionManager(NodeInfo localNode) {
        this.localNode = localNode;
    }

    public void setMessageHandler(Consumer<MessageProtocol> handler) {
        this.messageHandler = handler;
    }

    public void registerPeer(NodeInfo peer) {
        knownPeers.put(peer.getNodeId(), peer);
    }

    public void registerPeers(List<NodeInfo> peers) {
        peers.forEach(p -> knownPeers.put(p.getNodeId(), p));
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(localNode.getPort());
        serverSocket.setSoTimeout(1000);
        running = true;
        executor.submit(this::acceptLoop);
        logger.info("ConnectionManager listening on port {}", localNode.getPort());
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        connections.values().forEach(NodeConnection::close);
        connections.clear();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS))
                executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("ConnectionManager shut down");
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(Constants.READ_TIMEOUT_MS);
                executor.submit(() -> handleInboundSocket(socket));
            } catch (SocketTimeoutException ignored) {
            } catch (IOException e) {
                if (running) logger.warn("Accept error: {}", e.getMessage());
            }
        }
    }

    private void handleInboundSocket(Socket socket) {
        String remoteDesc = socket.getRemoteSocketAddress().toString();
        try {
            // ObjectOutputStream must be created and flushed before ObjectInputStream (deadlock avoidance)
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            MessageProtocol first = (MessageProtocol) in.readObject();
            String peerId = first.getSourceNodeId();

            NodeConnection conn = new NodeConnection(peerId, socket, out);
            connections.put(peerId, conn);
            logger.info("Inbound connection registered from {}", peerId);

            dispatch(first);

            while (running && !socket.isClosed()) {
                try {
                    MessageProtocol msg = (MessageProtocol) in.readObject();
                    dispatch(msg);
                } catch (SocketTimeoutException ignored) {
                }
            }
        } catch (EOFException | java.net.SocketException e) {
            logger.debug("Connection closed from {}", remoteDesc);
        } catch (Exception e) {
            logger.warn("Error reading from {}: {}", remoteDesc, e.getMessage());
        } finally {
            connections.values().removeIf(c -> c.getSocket() == socket);
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void dispatch(MessageProtocol msg) {
        if (messageHandler != null) {
            try {
                messageHandler.accept(msg);
            } catch (Exception e) {
                logger.error("Handler threw exception for {}: {}", msg.getType(), e.getMessage(), e);
            }
        }
    }

    public void sendMessage(String targetNodeId, MessageProtocol message) throws IOException {
        for (int attempt = 1; attempt <= Constants.MAX_RETRIES; attempt++) {
            try {
                NodeConnection conn = getOrCreateConnection(targetNodeId);
                conn.send(message);
                return;
            } catch (IOException e) {
                logger.warn("Send attempt {}/{} to {} failed: {}",
                        attempt, Constants.MAX_RETRIES, targetNodeId, e.getMessage());
                connections.remove(targetNodeId);
                if (attempt == Constants.MAX_RETRIES) throw e;
                try { Thread.sleep(100L * attempt); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }

    public void sendMessageAsync(String targetNodeId, MessageProtocol message) {
        executor.submit(() -> {
            try {
                sendMessage(targetNodeId, message);
            } catch (IOException e) {
                logger.warn("Async send to {} failed: {}", targetNodeId, e.getMessage());
            }
        });
    }

    private NodeConnection getOrCreateConnection(String targetNodeId) throws IOException {
        NodeConnection existing = connections.get(targetNodeId);
        if (existing != null && existing.isAlive()) return existing;

        NodeInfo peer = knownPeers.get(targetNodeId);
        if (peer == null)
            throw new IOException("Unknown peer: " + targetNodeId);

        return connectToPeer(peer);
    }

    private synchronized NodeConnection connectToPeer(NodeInfo peer) throws IOException {
        NodeConnection existing = connections.get(peer.getNodeId());
        if (existing != null && existing.isAlive()) return existing;

        logger.info("Establishing outbound connection to {}", peer);
        Socket socket = new Socket();
        socket.connect(new java.net.InetSocketAddress(peer.getHost(), peer.getPort()),
                Constants.CONNECTION_TIMEOUT_MS);
        socket.setSoTimeout(Constants.READ_TIMEOUT_MS);

        // Output stream first — matches inbound side, avoids duplicate stream header
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

        NodeConnection conn = new NodeConnection(peer.getNodeId(), socket, out);
        connections.put(peer.getNodeId(), conn);

        executor.submit(() -> readOutboundResponses(peer.getNodeId(), socket, in));

        logger.info("Connected to peer {}", peer.getNodeId());
        return conn;
    }

    private void readOutboundResponses(String peerId, Socket socket, ObjectInputStream in) {
        try {
            while (running && !socket.isClosed()) {
                try {
                    MessageProtocol msg = (MessageProtocol) in.readObject();
                    dispatch(msg);
                } catch (SocketTimeoutException ignored) {
                }
            }
        } catch (EOFException | java.net.SocketException e) {
            logger.debug("Outbound connection to {} closed", peerId);
        } catch (Exception e) {
            if (running) logger.warn("Error reading from outbound connection to {}: {}", peerId, e.getMessage());
        } finally {
            connections.remove(peerId);
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static final class NodeConnection {
        private final String             nodeId;
        private final Socket             socket;
        private final ObjectOutputStream out;

        NodeConnection(String nodeId, Socket socket, ObjectOutputStream out) {
            this.nodeId = nodeId;
            this.socket = socket;
            this.out    = out;
        }

        String getNodeId() { return nodeId; }
        Socket getSocket() { return socket; }

        boolean isAlive() {
            return socket != null && !socket.isClosed() && socket.isConnected();
        }

        synchronized void send(MessageProtocol msg) throws IOException {
            out.writeObject(msg);
            out.flush();
            out.reset();
        }

        void close() {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }
}
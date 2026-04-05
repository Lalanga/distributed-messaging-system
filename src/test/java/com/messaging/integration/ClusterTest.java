package com.messaging.integration;

import com.messaging.common.Message;
import com.messaging.common.NodeInfo;
import com.messaging.common.enums.NodeRole;
import com.messaging.config.ServerConfig;
import com.messaging.server.MessagingServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spins up a real 3-node cluster in-process and verifies basic operation.
 * Uses high ports (16xxx) to avoid conflicts with a running dev cluster.
 */
@DisplayName("Cluster integration tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterTest {

    @TempDir static Path tempDir;

    static MessagingServer s1, s2, s3;
    static ExecutorService exec = Executors.newFixedThreadPool(3);

    @BeforeAll
    static void startCluster() throws Exception {
        s1 = buildServer("s1", 16001, List.of(peer("s2", 16002), peer("s3", 16003)));
        s2 = buildServer("s2", 16002, List.of(peer("s1", 16001), peer("s3", 16003)));
        s3 = buildServer("s3", 16003, List.of(peer("s1", 16001), peer("s2", 16002)));

        exec.submit(() -> silentStart(s1));
        Thread.sleep(200);
        exec.submit(() -> silentStart(s2));
        Thread.sleep(200);
        exec.submit(() -> silentStart(s3));

        // Wait for servers to become ready
        waitForReady(s1, s2, s3);
    }

    @AfterAll
    static void stopCluster() {
        if (s1 != null) s1.shutdown();
        if (s2 != null) s2.shutdown();
        if (s3 != null) s3.shutdown();
        exec.shutdown();
    }

    @Test
    @Order(1)
    @DisplayName("All three servers reach ready state")
    void serversReady() {
        assertTrue(s1.isReady(), "s1 should be ready");
        assertTrue(s2.isReady(), "s2 should be ready");
        assertTrue(s3.isReady(), "s3 should be ready");
    }

    @Test
    @Order(2)
    @DisplayName("Exactly one leader is elected within 5 seconds")
    void exactlyOneLeader() throws InterruptedException {
        // Give Raft time to elect a leader
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = countLeaders();
            if (leaders == 1) return; // pass
            Thread.sleep(200);
        }
        assertEquals(1, countLeaders(), "Exactly one leader should be elected");
    }

    //  Helpers

    private long countLeaders() {
        return List.of(s1, s2, s3).stream().filter(MessagingServer::isLeader).count();
    }

    private static MessagingServer buildServer(String id, int port, List<NodeInfo> peers)
            throws IOException {
        ServerConfig cfg = new ServerConfig();
        cfg.setServerId(id);
        cfg.setPort(port);
        cfg.setPeers(peers);
        cfg.setDataDir(tempDir.resolve("data").toString());
        cfg.setHeartbeatInterval(200);
        cfg.setElectionTimeout(600);
        cfg.setReplicationFactor(3);
        cfg.setConsistencyModel("QUORUM");
        cfg.setEnableTimeSync(false); // skip NTP in tests
        return new MessagingServer(cfg);
    }

    private static NodeInfo peer(String id, int port) {
        return new NodeInfo(id, "localhost", port);
    }

    private static void silentStart(MessagingServer s) {
        try { s.start(); } catch (Exception e) { /* server was shut down */ }
    }

    private static void waitForReady(MessagingServer... servers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            boolean allReady = true;
            for (MessagingServer s : servers) {
                if (!s.isReady()) { allReady = false; break; }
            }
            if (allReady) return;
            Thread.sleep(200);
        }
    }
}

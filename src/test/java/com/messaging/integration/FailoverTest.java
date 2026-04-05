package com.messaging.integration;

import com.messaging.common.NodeInfo;
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
 * Verifies that the cluster survives a node failure and re-elects a leader.
 */
@DisplayName("Failover integration tests")
class FailoverTest {

    @TempDir Path tempDir;

    private MessagingServer s1, s2, s3;
    private ExecutorService exec;

    @BeforeEach
    void startCluster() throws Exception {
        exec = Executors.newFixedThreadPool(3);
        s1 = build("fs1", 17001, List.of(peer("fs2", 17002), peer("fs3", 17003)));
        s2 = build("fs2", 17002, List.of(peer("fs1", 17001), peer("fs3", 17003)));
        s3 = build("fs3", 17003, List.of(peer("fs1", 17001), peer("fs2", 17002)));

        exec.submit(() -> silent(s1));
        Thread.sleep(200);
        exec.submit(() -> silent(s2));
        Thread.sleep(200);
        exec.submit(() -> silent(s3));

        waitReady(8000, s1, s2, s3);
    }

    @AfterEach
    void stopCluster() {
        if (s1 != null) s1.shutdown();
        if (s2 != null) s2.shutdown();
        if (s3 != null) s3.shutdown();
        exec.shutdown();
    }

    @Test
    @DisplayName("Cluster has exactly one leader at startup")
    void oneLeaderAtStart() throws InterruptedException {
        waitForLeader(5000);
        assertEquals(1, leaderCount(), "Should have exactly one leader");
    }

    @Test
    @DisplayName("New leader is elected after current leader is killed")
    void newLeaderAfterFailure() throws InterruptedException {
        // Wait for initial leader
        waitForLeader(5000);
        assertEquals(1, leaderCount());

        // Kill the leader
        MessagingServer leader = findLeader();
        assertNotNull(leader);
        leader.shutdown();

        // Remaining two nodes should elect a new leader
        Thread.sleep(500); // brief pause for failure detection
        long deadline = System.currentTimeMillis() + 6000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = List.of(s1, s2, s3).stream()
                    .filter(s -> s != leader && s.isLeader())
                    .count();
            if (leaders == 1) return; // test passes
            Thread.sleep(200);
        }
        fail("No new leader elected within 6 seconds after killing old leader");
    }

    @Test
    @DisplayName("Cluster tolerates minority (1 of 3) node failure")
    void clusterToleratesMinorityFailure() throws InterruptedException {
        waitForLeader(5000);

        // Kill a follower
        MessagingServer follower = findFollower();
        if (follower != null) follower.shutdown();

        // The remaining two nodes should still have a leader
        Thread.sleep(1000);
        long leaders = List.of(s1, s2, s3).stream()
                .filter(s -> s != follower && s.isLeader())
                .count();
        assertEquals(1, leaders,
                "Cluster with 2 of 3 nodes alive should maintain exactly one leader");
    }

    //  Helpers

    private long leaderCount() {
        return List.of(s1, s2, s3).stream().filter(MessagingServer::isLeader).count();
    }

    private MessagingServer findLeader() {
        return List.of(s1, s2, s3).stream()
                .filter(MessagingServer::isLeader).findFirst().orElse(null);
    }

    private MessagingServer findFollower() {
        return List.of(s1, s2, s3).stream()
                .filter(s -> !s.isLeader() && s.isReady()).findFirst().orElse(null);
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (leaderCount() == 1) return;
            Thread.sleep(200);
        }
    }

    private MessagingServer build(String id, int port, List<NodeInfo> peers) throws IOException {
        ServerConfig cfg = new ServerConfig();
        cfg.setServerId(id);
        cfg.setPort(port);
        cfg.setPeers(peers);
        cfg.setDataDir(tempDir.resolve("data").toString());
        cfg.setHeartbeatInterval(200);
        cfg.setElectionTimeout(600);
        cfg.setReplicationFactor(3);
        cfg.setConsistencyModel("QUORUM");
        cfg.setEnableTimeSync(false);
        return new MessagingServer(cfg);
    }

    private static NodeInfo peer(String id, int port) {
        return new NodeInfo(id, "localhost", port);
    }

    private static void silent(MessagingServer s) {
        try { s.start(); } catch (Exception ignored) {}
    }

    private static void waitReady(long timeoutMs, MessagingServer... servers)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (MessagingServer s : servers) if (!s.isReady()) { all = false; break; }
            if (all) return;
            Thread.sleep(200);
        }
    }
}

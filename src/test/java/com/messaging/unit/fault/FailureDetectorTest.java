package com.messaging.unit.fault;

import com.messaging.common.NodeInfo;
import com.messaging.fault.FailureDetector;
import org.junit.jupiter.api.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FailureDetector unit tests")
class FailureDetectorTest {

    private static final int HB_MS = 200;
    private NodeInfo local;
    private FailureDetector detector;

    @BeforeEach
    void setup() {
        local = new NodeInfo("local", "localhost", 5000);
        NodeInfo peer = new NodeInfo("peer1", "localhost", 5001);
        detector = new FailureDetector(local, List.of(peer), HB_MS);
        detector.start();
    }

    @AfterEach
    void tearDown() {
        detector.shutdown();
    }

    @Test
    @DisplayName("Peer is alive immediately after heartbeat")
    void peerAliveAfterHeartbeat() {
        detector.recordHeartbeat("peer1");
        assertTrue(detector.isAlive("peer1"));
    }

    @Test
    @DisplayName("Peer is marked dead after missing threshold heartbeats")
    void peerDeadAfterTimeout() throws InterruptedException {
        // Don't send any heartbeats, wait for 4x interval
        Thread.sleep((long) HB_MS * 4);
        assertFalse(detector.isAlive("peer1"), "Peer should be presumed dead after missing heartbeats");
    }

    @Test
    @DisplayName("Failure callback fires when node times out")
    void failureCallbackFires() throws InterruptedException {
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        detector.setOnFailure(id -> callbackFired.set(true));
        Thread.sleep((long) HB_MS * 4);
        assertTrue(callbackFired.get(), "onFailure callback should have fired");
    }

    @Test
    @DisplayName("Recovery callback fires when dead node sends heartbeat")
    void recoveryCallbackFires() throws InterruptedException {
        AtomicBoolean recovered = new AtomicBoolean(false);
        detector.setOnRecovery(id -> recovered.set(true));

        // Let it go dead first
        Thread.sleep((long) HB_MS * 4);
        assertFalse(detector.isAlive("peer1"));

        // Now send a heartbeat - should trigger recovery
        detector.recordHeartbeat("peer1");
        assertTrue(recovered.get(), "onRecovery callback should have fired");
        assertTrue(detector.isAlive("peer1"), "Peer should be alive again");
    }

    @Test
    @DisplayName("getDeadNodes returns only dead nodes")
    void getDeadNodesIsAccurate() throws InterruptedException {
        Thread.sleep((long) HB_MS * 4);
        assertTrue(detector.getDeadNodes().contains("peer1"));

        detector.recordHeartbeat("peer1");
        assertFalse(detector.getDeadNodes().contains("peer1"));
    }
}
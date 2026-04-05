package com.messaging.unit.timesync;

import com.messaging.common.Message;
import com.messaging.timesync.MessageReorderer;
import com.messaging.timesync.TimestampCorrector;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Time sync unit tests")
class TimeSyncTest {

    //  TimestampCorrector

    @Test
    @DisplayName("Message within skew tolerance keeps client timestamp")
    void withinToleranceKeepsClientTs() {
        TimestampCorrector corrector = new TimestampCorrector();
        Message m = new Message("hi", "alice", "bob");
        long serverTime = m.getClientTimestamp() + 100; // 100ms diff — within 5s tolerance
        corrector.correct(m, serverTime);
        assertEquals(m.getClientTimestamp(), m.getServerTimestamp(),
                "Small skew: server should use client timestamp");
    }

    @Test
    @DisplayName("Message with excessive skew gets server timestamp")
    void excessiveSkewUsesServerTs() {
        TimestampCorrector corrector = new TimestampCorrector();
        Message m = new Message("hi", "alice", "bob");
        long serverTime = m.getClientTimestamp() + 10_000L; // 10s diff — exceeds 5s max
        corrector.correct(m, serverTime);
        assertEquals(serverTime, m.getServerTimestamp(),
                "Large skew: server should override with its own time");
    }

    @Test
    @DisplayName("Offset is stored after correction")
    void offsetStoredAfterCorrection() {
        TimestampCorrector corrector = new TimestampCorrector();
        Message m = new Message("hi", "alice", "bob");
        long serverTime = m.getClientTimestamp() + 10_000L;
        corrector.correct(m, serverTime);
        assertEquals(10_000L, corrector.getOffset("alice"),
                "Offset for sender should be recorded");
    }

    //  MessageReorderer

    @Test
    @DisplayName("Messages are drained in timestamp order")
    void drainedInOrder() throws InterruptedException {
        MessageReorderer reorderer = new MessageReorderer();
        long base = System.currentTimeMillis() - 5000L; // old enough to be drainable

        Message m1 = makeMsg("alice", "bob", base + 200);
        Message m2 = makeMsg("alice", "bob", base + 100); // arrives second but older
        Message m3 = makeMsg("alice", "bob", base + 300);

        reorderer.enqueue(m2);
        reorderer.enqueue(m1);
        reorderer.enqueue(m3);

        List<Message> drained = reorderer.drain("bob", System.currentTimeMillis());
        assertEquals(3, drained.size());
        assertTrue(drained.get(0).getServerTimestamp() <= drained.get(1).getServerTimestamp(),
                "First drained message should have smaller or equal timestamp");
        assertTrue(drained.get(1).getServerTimestamp() <= drained.get(2).getServerTimestamp());
    }

    @Test
    @DisplayName("Recent messages are held back inside the reorder window")
    void recentMessagesHeldBack() {
        MessageReorderer reorderer = new MessageReorderer();
        // Timestamp is NOW — should not be drained yet (inside 1s window)
        Message m = makeMsg("alice", "bob", System.currentTimeMillis());
        reorderer.enqueue(m);

        List<Message> drained = reorderer.drain("bob", System.currentTimeMillis());
        assertEquals(0, drained.size(), "Very recent message should be held back");
        assertEquals(1, reorderer.pendingCount("bob"));
    }

    @Test
    @DisplayName("pendingCount is zero after draining all messages")
    void pendingCountZeroAfterDrain() {
        MessageReorderer reorderer = new MessageReorderer();
        long old = System.currentTimeMillis() - 5000L;
        reorderer.enqueue(makeMsg("alice", "bob", old));
        reorderer.drain("bob", System.currentTimeMillis());
        assertEquals(0, reorderer.pendingCount("bob"));
    }

    //  Helper

    private Message makeMsg(String from, String to, long serverTs) {
        Message m = new Message("content", from, to);
        m.setServerTimestamp(serverTs);
        return m;
    }
}

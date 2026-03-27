package com.messaging.unit.consensus;

import com.messaging.common.NodeInfo;
import com.messaging.common.enums.NodeRole;
import com.messaging.consensus.raft.RaftNode;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RaftNode unit tests")
class RaftNodeTest {

    @TempDir Path tempDir;

    private RaftNode makeNode(String id, List<NodeInfo> peers) {
        NodeInfo local = new NodeInfo(id, "localhost", 5000);
        return new RaftNode(local, peers, 1500, tempDir.toString());
    }

    @Test
    @DisplayName("New node starts as FOLLOWER with term 0")
    void initialState() {
        RaftNode node = makeNode("n1", Collections.emptyList());
        node.start();
        assertEquals(NodeRole.FOLLOWER, node.getRole());
        assertEquals(0L, node.getCurrentTerm());
        node.shutdown();
    }

    @Test
    @DisplayName("Single-node cluster elects itself LEADER")
    void singleNodeBecomesLeader() throws InterruptedException {
        RaftNode node = makeNode("n1", Collections.emptyList());
        node.start();
        // Election timeout fires after ~1500ms; give 3s
        Thread.sleep(3000);
        assertEquals(NodeRole.LEADER, node.getRole(),
                "Single node should win its own election");
        node.shutdown();
    }

    @Test
    @DisplayName("appendCommand returns -1 when not leader")
    void appendCommandFailsIfNotLeader() {
        RaftNode node = makeNode("n1", List.of(new NodeInfo("n2", "localhost", 5002)));
        node.start();
        long idx = node.appendCommand("hello".getBytes());
        assertEquals(-1L, idx, "Non-leader must not accept commands");
        node.shutdown();
    }

    @Test
    @DisplayName("Term increases after each election attempt")
    void termIncreases() throws InterruptedException {
        NodeInfo peer = new NodeInfo("n2", "localhost", 5002);
        RaftNode node = makeNode("n1", List.of(peer));
        node.start();
        Thread.sleep(2000); // let at least one election fire
        assertTrue(node.getCurrentTerm() >= 1,
                "Term should increment after an election attempt");
        node.shutdown();
    }

    @Test
    @DisplayName("Log size grows when leader appends")
    void logGrowsOnAppend() throws InterruptedException {
        // Single-node so it will self-elect
        RaftNode node = makeNode("n1", Collections.emptyList());
        node.start();
        Thread.sleep(3000); // wait for leader election
        assertEquals(NodeRole.LEADER, node.getRole());

        // noop is already in log from becomeLeader(); append two more
        node.appendCommand("msg1".getBytes());
        node.appendCommand("msg2".getBytes());
        assertTrue(node.getLogSize() >= 3,
                "Log should contain noop + 2 appended entries");
        node.shutdown();
    }
}

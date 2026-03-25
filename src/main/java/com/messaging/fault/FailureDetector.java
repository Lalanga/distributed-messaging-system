package com.messaging.fault;

import java.util.HashMap;
import java.util.Map;

/*
 * Name: Vimukthi Munasinghe
 * IT Number: IT24610782
 * Task: Implementing Failure Detection Logic (Lab 3 & 4)
 * This class keeps track of peer nodes and detects if any of them have crashed.
 */
public class FailureDetector {

    // A map to store node names and the last time (in milliseconds) they sent a heartbeat
    private Map<String, Long> nodeHealthMap = new HashMap<>();

    // Timeout duration set to 15 seconds as per project requirements
    private final long MAX_WAIT_TIME = 15000;

    /*
     * This method is called by the network layer whenever a heartbeat
     * signal is received from another server.
     */
    public void updateHeartbeat(String id) {
        long receivedTime = System.currentTimeMillis();
        nodeHealthMap.put(id, receivedTime);

        // Debugging message to see if it's working in the console
        System.out.println("[INFO] Heartbeat received from Node: " + id + " at " + new java.util.Date());
    }

    /*
     * This method scans all nodes in the map to check if any node has
     * exceeded the 15-second timeout limit.
     */
    public void checkForDeadNodes() {
        long currentTime = System.currentTimeMillis();

        for (String nodeId : nodeHealthMap.keySet()) {
            long lastActive = nodeHealthMap.get(nodeId);
            long timeGap = currentTime - lastActive;

            // Checking if the node is silent for too long
            if (timeGap > MAX_WAIT_TIME) {
                System.err.println("[ALERT] Node Failure Detected! ID: " + nodeId + " | Inactive for: " + (timeGap/1000) + " seconds");
                triggerRecovery(nodeId);
            }
        }
    }

    private void triggerRecovery(String failedNode) {
        // This is where we will call the FailoverManager later
        System.out.println("Initiating failover for failed node: " + failedNode);

        // TODO: Ex1 - Remove the failed node from active list and alert ReplicationManager
    }
}
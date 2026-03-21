package com.messaging.fault;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * Student Name: Vimukthi Munasinghe
 * Registration Number: IT24610782
 * Task: Fault Tolerance - Heartbeat Manager
 * * This class is responsible for sending periodic "ALIVE" signals
 * to other server nodes in the distributed system.
 */
public class HeartbeatManager {

    private ScheduledExecutorService heartbeatTask;
    private List<String> allNodes = new ArrayList<>();

    // Constructor to initialize the server list
    public HeartbeatManager(List<String> nodes) {
        this.allNodes = nodes;
        // Creating a single thread to handle the background heartbeat task
        this.heartbeatTask = Executors.newSingleThreadScheduledExecutor();
    }

    // Method to start sending heartbeats every 5 seconds
    public void startService() {
        System.out.println(">>> Heartbeat Service Started for Node: IT24610782");

        // Lab 2 & 7: Scheduling the task to run repeatedly
        heartbeatTask.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                sendSignals();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    // Iterating through the list of peers to send the signal
    private void sendSignals() {
        for (String targetNode : allNodes) {
            // For now, printing to console.
            // TODO: Ex1 - Implement Socket connection to send actual packet (Lab 2)
            System.out.println("Sending ALIVE signal to peer node at: " + targetNode);
        }
    }

    // Shutdown the service when the server stops
    public void stopService() {
        if (heartbeatTask != null) {
            heartbeatTask.shutdown();
        }
    }
}
package com.messaging.fault;

/*
 * Name: Vimukthi Munasinghe
 * IT No: IT24610782
 * Task: Testing Failure Detection Logic for Lab Submission.
 */
public class FaultToleranceTest {

    public static void main(String[] args) {
        FailureDetector detector = new FailureDetector();

        System.out.println("========== DISTRIBUTED SYSTEM: FAULT TOLERANCE TEST ==========");

        detector.updateHeartbeat("Server-Alpha");

        try {
            System.out.println("\n[TEST] Waiting 5 seconds... (Checking for false alarms)");
            Thread.sleep(5000);
            detector.checkForDeadNodes();

            System.out.println("\n[TEST] Waiting 11 more seconds... (Simulating a real crash)");
            Thread.sleep(11000);
            detector.checkForDeadNodes();

        } catch (InterruptedException e) {
            System.err.println("Test interrupted: " + e.getMessage());
        }

        System.out.println("\n========== TEST SEQUENCE COMPLETED ==========");
    }
}
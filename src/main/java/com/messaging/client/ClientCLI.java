package com.messaging.client;
import com.messaging.common.Message;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
public class ClientCLI {
    private static final Logger logger = LoggerUtil.getLogger(ClientCLI.class);
    private MessagingClient client;
    private Scanner scanner;
    private volatile boolean running = true;
    public void start() {
        client = new MessagingClient();
        scanner = new Scanner(System.in);
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║ Distributed Messaging System Client ║");
        System.out.println("╚══════════════════════════════════════╝");
        if (!connectToServer()) return;
        while (running) {
            printMenu();
            String choice = scanner.nextLine().trim();
            switch (choice) {
                case "1":
                    doSend();
                    break;
                case "2":
                    doGetMessages();
                    break;
                case "3":
                    doShowId();
                    break;
                case "4":
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice — enter 1, 2, 3, or 4.");
            }
        }
        client.disconnect();
        System.out.println("Goodbye!");
    }
    // ----------------------------------------------------------------
    // Connect
    // ----------------------------------------------------------------
    private boolean connectToServer() {
        System.out.print("Server host [localhost]: ");
        String host = scanner.nextLine().trim();
        if (host.isEmpty()) host = "localhost";
        System.out.print("Server port [5001]: ");
        String portStr = scanner.nextLine().trim();
        int port = portStr.isEmpty() ? 5001 : Integer.parseInt(portStr);
        try {
            client.connect(host, port);
            System.out.printf("Connected as %s%n%n", client.getClientId());
            return true;
        } catch (IOException e) {
            System.out.println("Connection failed: " + e.getMessage());
            logger.error("Connection failed", e);
            return false;
        }
    }
    // ----------------------------------------------------------------
    // Menu actions
    // ----------------------------------------------------------------
    private void printMenu() {
        System.out.println("\n─────────────────────────");
        System.out.println(" 1. Send message");
        System.out.println(" 2. Check my messages");
        System.out.println(" 3. Show my client ID");
        System.out.println(" 4. Exit");
        System.out.print("Choice: ");
    }
    private void doSend() {
        System.out.print("Recipient ID: ");
        String to = scanner.nextLine().trim();
        if (to.isEmpty()) {
            System.out.println("Recipient cannot be empty.");
            return;
        }
        System.out.print("Message: ");
        String content = scanner.nextLine();
        if (content.isEmpty()) {
            System.out.println("Message cannot be empty.");
            return;
        }
        try {
            boolean ok = client.sendMessage(to, content);
            System.out.println(ok ? " Message sent." : " Send failed — server rejected.");
        } catch (IOException e) {
            System.out.println(" Send error: " + e.getMessage());
        }
    }
    private void doGetMessages() {
        System.out.print("How many messages to fetch [10]: ");
        String limitStr = scanner.nextLine().trim();
        int limit = limitStr.isEmpty() ? 10 : Integer.parseInt(limitStr);
        try {
            List<Message> messages = client.getMessages(limit);
            if (messages.isEmpty()) {
                System.out.println("No messages.");
            } else {
                System.out.printf("%n%-38s %-15s %s%n", "ID", "From", "Content");
                System.out.println("─".repeat(80));
                for (Message m : messages) {
                    System.out.printf("%-38s %-15s %s%n",
                            m.getMessageId().substring(0, 8) + "...",
                            m.getSenderId(),
                            m.getContent());
                }
            }
        } catch (IOException e) {
            System.out.println(" Fetch error: " + e.getMessage());
        }
    }
    private void doShowId() {
        System.out.println("Your client ID: " + client.getClientId());
    }
}
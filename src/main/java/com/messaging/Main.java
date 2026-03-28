package com.messaging;
import com.messaging.client.ClientCLI;
import com.messaging.config.ServerConfig;
import com.messaging.server.MessagingServer;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
public class Main {
    private static final Logger logger = LoggerUtil.getLogger(Main.class);
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }
        String mode = args[0].toLowerCase();
        switch (mode) {
            case "server":
                runServer(args);
                break;
            case "client":
                runClient();
                break;
            default:
                System.out.println("Unknown mode: " + mode);
                printUsage();
                System.exit(1);
        }
    }
    private static void runServer(String[] args) {
        if (args.length < 2) {
            System.out.println("Error: server mode requires a config file path.");
            printUsage();
            System.exit(1);
        }
        String configPath = args[1];
        MessagingServer server = null;
        try {
            ServerConfig config = ServerConfig.load(configPath);
            server = new MessagingServer(config);
            // Graceful shutdown on SIGINT / SIGTERM
            final MessagingServer finalServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook triggered");
                finalServer.shutdown();
            }, "shutdown-hook"));
            server.start(); // blocks until running flag cleared
        } catch (java.io.FileNotFoundException e) {
            System.out.println("Config file not found: " + configPath);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Server failed: {}", e.getMessage(), e);
            if (server != null) server.shutdown();
            System.exit(1);
        }
    }
    private static void runClient() {
        try {
            new ClientCLI().start();
        } catch (Exception e) {
            logger.error("Client error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println(" java -jar messaging-system.jar server <config.json>");
        System.out.println(" java -jar messaging-system.jar client");
    }
}
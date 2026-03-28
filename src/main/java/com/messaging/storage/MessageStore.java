package com.messaging.storage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.messaging.common.Message;
import com.messaging.util.Constants;
import com.messaging.util.logger.LoggerUtil;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
public class MessageStore {
    private static final Logger logger = LoggerUtil.getLogger(MessageStore.class);
    private final String dataDir;
    private final Map<String, Message> messages = new ConcurrentHashMap<>();
    private final Map<String, List<String>> recipientIndex = new
            ConcurrentHashMap<>();
    private final Deduplicator deduplicator = new Deduplicator();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Gson gson = new GsonBuilder().create();
    private static final String MESSAGES_FILE = "messages.json";
    private static final String INDEX_FILE = "index.json";
    public MessageStore(String baseDataDir, String nodeId) {
        this.dataDir = baseDataDir + "/" + nodeId;
    }
    public void start() {
        try {
            Files.createDirectories(Paths.get(dataDir));
            loadFromDisk();
            logger.info("MessageStore ready at {} ({} messages)", dataDir,
                    messages.size());
        } catch (IOException e) {
            logger.error("Failed to initialise MessageStore: {}", e.getMessage(),
                    e);
        }
    }
    public void shutdown() {
        saveToDisk();
        deduplicator.shutdown();
        logger.info("MessageStore shut down — {} messages persisted",
                messages.size());
    }
    /**
     * Store a message.
     *
     * @return true if stored successfully,
     * false if duplicate or failed
     */
    public boolean storeMessage(Message message) {
        lock.writeLock().lock();
        try {
            if (deduplicator.isDuplicate(message.getMessageId())) {
                logger.debug("Duplicate dropped: {}", message.getMessageId());
                return false;
            }
            messages.put(message.getMessageId(), message);
            recipientIndex
                    .computeIfAbsent(message.getRecipientId(), k -> new
                            ArrayList<>())
                    .add(message.getMessageId());
            deduplicator.markProcessed(message.getMessageId());
            logger.debug("Stored: {}", message.getMessageId());
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }
    public boolean deleteMessage(String messageId) {
        lock.writeLock().lock();
        try {
            Message m = messages.remove(messageId);
            if (m != null) {
                List<String> idx = recipientIndex.get(m.getRecipientId());
                if (idx != null) {
                    idx.remove(messageId);
                }
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    public Message getMessage(String messageId) {
        lock.readLock().lock();
        try {
            return messages.get(messageId);
        } finally {
            lock.readLock().unlock();
        }
    }
    public List<Message> getMessagesForRecipient(String recipientId, int limit) {
        lock.readLock().lock();
        try {
            List<String> ids = recipientIndex.get(recipientId);
            if (ids == null || ids.isEmpty()) {
                return Collections.emptyList();
            }
            return ids.stream()
                    .map(messages::get)
                    .filter(Objects::nonNull)

                    .sorted(Comparator.comparingLong(Message::getServerTimestamp).reversed())
                    .limit(Math.min(limit, Constants.MAX_MESSAGES_PER_QUERY))
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    public List<Message> getAllMessages() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(messages.values());
        } finally {
            lock.readLock().unlock();
        }
    }
    public int getMessageCount() {
        return messages.size();
    }
    private void loadFromDisk() {
        Path mf = Paths.get(dataDir, MESSAGES_FILE);
        Path ixf = Paths.get(dataDir, INDEX_FILE);
        try {
            if (Files.exists(mf)) {
                String json = new String(Files.readAllBytes(mf),
                        StandardCharsets.UTF_8);
                Type t = new TypeToken<Map<String, Message>>() {}.getType();
                Map<String, Message> loaded = gson.fromJson(json, t);
                if (loaded != null) {
                    messages.putAll(loaded);
                }
            }
            if (Files.exists(ixf)) {
                String json = new String(Files.readAllBytes(ixf),
                        StandardCharsets.UTF_8);
                Type t = new TypeToken<Map<String, List<String>>>() {}.getType();
                Map<String, List<String>> loaded = gson.fromJson(json, t);
                if (loaded != null) {
                    recipientIndex.putAll(loaded);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load data from disk: {}", e.getMessage(), e);
        }
    }
    private void saveToDisk() {
        lock.readLock().lock();
        try {
            Files.write(
                    Paths.get(dataDir, MESSAGES_FILE),
                    gson.toJson(messages).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            Files.write(
                    Paths.get(dataDir, INDEX_FILE),
                    gson.toJson(recipientIndex).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            logger.debug("Persisted {} messages to disk", messages.size());
        } catch (IOException e) {
            logger.error("Failed to save data to disk: {}", e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }
}
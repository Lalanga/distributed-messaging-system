package com.messaging.unit.replication;
import com.messaging.common.Message;
import com.messaging.storage.MessageStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
@DisplayName("MessageStore unit tests")
class MessageStoreTest {
    @TempDir Path tempDir;
    private MessageStore store;
    @BeforeEach
    void setUp() {
        store = new MessageStore(tempDir.toString(), "node1");
        store.start();
    }
    @AfterEach
    void tearDown() { store.shutdown(); }
    @Test
    @DisplayName("Stored message can be retrieved by ID")
    void storeAndRetrieve() {
        Message m = new Message("hello", "alice", "bob");
        m.setServerTimestamp(System.currentTimeMillis());
        assertTrue(store.storeMessage(m));
        assertEquals(m.getContent(),
                store.getMessage(m.getMessageId()).getContent());
    }
    @Test
    @DisplayName("Duplicate message is rejected")
    void duplicateRejected() {
        Message m = new Message("hello", "alice", "bob");
        m.setServerTimestamp(System.currentTimeMillis());
        assertTrue(store.storeMessage(m));
        assertFalse(store.storeMessage(m), "Second store of same ID should return false");
        assertEquals(1, store.getMessageCount());
    }
    @Test
    @DisplayName("getMessagesForRecipient returns correct messages")
    void getForRecipient() {
        for (int i = 0; i < 5; i++) {
            Message m = new Message("msg" + i, "sender", "bob");
            m.setServerTimestamp(System.currentTimeMillis() + i);
            store.storeMessage(m);
        }
        List<Message> msgs = store.getMessagesForRecipient("bob", 10);
        assertEquals(5, msgs.size());
    }
    @Test
    @DisplayName("Limit is respected in getMessagesForRecipient")
    void limitRespected() {
        for (int i = 0; i < 10; i++) {
            Message m = new Message("msg" + i, "sender", "carol");
            m.setServerTimestamp(System.currentTimeMillis() + i);
            store.storeMessage(m);
        }
        List<Message> msgs = store.getMessagesForRecipient("carol", 3);
        assertEquals(3, msgs.size());
    }
    @Test
    @DisplayName("deleteMessage removes the message")
    void deleteMessage() {
        Message m = new Message("bye", "alice", "bob");
        m.setServerTimestamp(System.currentTimeMillis());
        store.storeMessage(m);
        assertTrue(store.deleteMessage(m.getMessageId()));
        assertNull(store.getMessage(m.getMessageId()));
        assertEquals(0, store.getMessageCount());
    }
    @Test
    @DisplayName("Messages persist across restart")
    void persistsAcrossRestart() {
        Message m = new Message("persisted", "alice", "bob");
        m.setServerTimestamp(System.currentTimeMillis());
        store.storeMessage(m);
        store.shutdown();
        MessageStore store2 = new MessageStore(tempDir.toString(), "node1");
        store2.start();
        assertNotNull(store2.getMessage(m.getMessageId()),
                "Message should survive a restart");
        store2.shutdown();
    }
}
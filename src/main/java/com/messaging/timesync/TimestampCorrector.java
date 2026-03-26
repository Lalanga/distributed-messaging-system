package com.messaging.timesync;
import com.messaging.common.Message;
import com.messaging.util.Constants;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
public class TimestampCorrector {
    private final Map<String, Long> offsets = new ConcurrentHashMap<>();
    public void updateOffset(String nodeId, long offset) {
        offsets.put(nodeId, offset);
    }
    /**
     * Applies timestamp correction to a message.
     * If clock skew is small, keep client timestamp.
     * Otherwise, use server time and record offset.
     */
    public void correct(Message message, long serverTimeMs) {
        long clientTime = message.getClientTimestamp();
        long skew = Math.abs(serverTimeMs - clientTime);
        if (skew > Constants.MAX_CLOCK_SKEW_MS) {
            message.setServerTimestamp(serverTimeMs);
            offsets.put(message.getSenderId(), serverTimeMs - clientTime);
        } else {
            message.setServerTimestamp(clientTime);
        }
    }
    public long getOffset(String nodeId) {
        return offsets.getOrDefault(nodeId, 0L);
    }
    public boolean hasExcessiveSkew(String nodeId, long timestamp, long serverTime)
    {
        Long offset = offsets.get(nodeId);
        long adjusted = (offset != null)
                ? timestamp + offset
                : timestamp;
        return Math.abs(adjusted - serverTime) > Constants.MAX_CLOCK_SKEW_MS;
    }
}

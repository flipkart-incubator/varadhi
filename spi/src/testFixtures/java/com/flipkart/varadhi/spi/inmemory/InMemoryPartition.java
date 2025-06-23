package com.flipkart.varadhi.spi.inmemory;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * InMemoryPartition is a simple in-memory implementation of a partition for testing purposes.
 * It stores messages in a list and provides methods to add messages and retrieve the latest offset.
 *
 * It maintains a list of segment of messages, where the segments can be of max size 1024. After 1024, a new segment of
 * capacity 1024 is added to the list.
 *
 * The first message is at offset `baseOffset`.
 *
 * It also allows registration of consumers to receive messages from the partition. There is a method called "clear" which
 * can remove unused segments if all the registered consumers have consumed all the messages from that segment.
 *
 * Consumers can be registered by their name and initial offset. If initial offset is less than the base offset, then
 * clamp it to base offset. Similarly, if initial offset is greater than the latest offset, then clamp it to latest
 * offset.
 */

@ThreadSafe
public class InMemoryPartition {

    record PersistedMessage(byte[] message, long timestamp) {
    }


    record Segment(List<PersistedMessage> messages) {
        Segment() {
            this(new ArrayList<>(1024));
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Segment> messages = new ArrayList<>();
    private final Map<String, Integer> consumerOffsets = new ConcurrentHashMap<>();
    private int baseOffset = 0;

    public int add(byte[] message) {
        lock.lock();
        try {
            Segment lastSegment;
            if (messages.isEmpty()) {
                lastSegment = new Segment();
                messages.add(lastSegment);
            } else {
                lastSegment = messages.getLast();
            }

            if (lastSegment.messages().size() == 1024) {
                lastSegment = new Segment();
                messages.add(lastSegment);
            } else if (lastSegment.messages().size() > 1024) {
                throw new IllegalStateException(
                    "Last segment is corrupted, it should not have more than 1024 messages."
                );
            }

            lastSegment.messages().add(new PersistedMessage(message, System.currentTimeMillis()));
            return getLatestOffset();
        } finally {
            lock.unlock();
        }
    }

    public int getLatestOffset() {
        return baseOffset + getTotalMessageCount() - 1;
    }

    public void clear() {
        lock.lock();
        try {
            if (messages.isEmpty()) {
                return;
            }

            // if consumer are empty, then clear will clear every full segment. if last segment is not full, then keep
            // the last segment.
            if (consumerOffsets.isEmpty()) {
                // Fast path: If we have multiple segments, try to clear full segments
                int segmentsCount = messages.size();
                if (segmentsCount == 1) {
                    return; // Keep at least one segment
                }
            }

            // Consumer-based clearing
            int minOffset = consumerOffsets.values().stream().min(Integer::compareTo).orElse(getLatestOffset());
            if (minOffset <= baseOffset) {
                return;
            }

            int messagesToClear = minOffset - baseOffset;
            int segmentsToClear = messagesToClear / 1024;

            if (segmentsToClear > 0) {
                // Efficient batch removal
                messages.subList(0, segmentsToClear).clear();
                baseOffset += segmentsToClear * 1024;
            }
        } finally {
            lock.unlock();
        }
    }

    public void registerConsumer(String consumerName, int initialOffset) {
        lock.lock();
        try {
            if (consumerOffsets.containsKey(consumerName)) {
                throw new DuplicateResourceException("Consumer " + consumerName + " already registered.");
            }
            int latestOffset = getLatestOffset();
            int clampedOffset = initialOffset;
            if (getTotalMessageCount() == 0) {
                clampedOffset = baseOffset;
            } else {
                clampedOffset = Math.max(clampedOffset, baseOffset);
                clampedOffset = Math.min(clampedOffset, latestOffset);
            }
            consumerOffsets.put(consumerName, clampedOffset);
        } finally {
            lock.unlock();
        }
    }

    public void unregisterConsumer(String consumerName) {
        lock.lock();
        try {
            if (consumerOffsets.remove(consumerName) == null) {
                throw new ResourceNotFoundException("Consumer " + consumerName + " not found.");
            }
        } finally {
            lock.unlock();
        }
    }

    private int getTotalMessageCount() {
        int segments = messages.size();
        return segments == 0 ? 0 : (segments - 1) * 1024 + messages.get(segments - 1).messages().size();
    }
}

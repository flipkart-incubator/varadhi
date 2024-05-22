package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message source that maintains ordering among messages of the same groupId.
 */
@RequiredArgsConstructor
public class GroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final ConcurrentHashMap<String, GroupTracker> allGroupedMessages = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<String> freeGroups = new ConcurrentLinkedDeque<>();

    /**
     * Maintains the count of total messages read from the consumer so far.
     * Required for watermark checks, for when this value runs low we can fetch more messages from the consumer.
     * Counter gets decremented when the message is committed/consumed.
     */
    private final AtomicLong totalInFlightMessages = new AtomicLong(0);

    // Used for watermark checks against the totalInFlightMessages. Will be driven via consumer configuration.
    private final long maxInFlightMessages = 100; // todo(aayush): make configurable

    private final Consumer<O> consumer;

    /**
     * Attempt to fill the message array with one message from each group.
     * Subsequent messages from a group are not fetched until the previous message is consumed.
     *
     * @param messages Array of message trackers to populate.
     *
     * @return CompletableFuture that completes when the messages are fetched.
     */
    @Override
    public CompletableFuture<Integer> nextMessages(MessageTracker[] messages) {
        if (!hasMaxInFlightMessages()) {
            return replenishAvailableGroups().thenApply(v -> nextMessagesInternal(messages));
        }
        return CompletableFuture.supplyAsync(() -> nextMessagesInternal(messages));
    }

    private int nextMessagesInternal(MessageTracker[] messages) {
        int i = 0;
        GroupTracker groupTracker;
        while (i < messages.length && (groupTracker = getGroupTracker()) != null) {
            messages[i++] = new GroupedMessageTracker(groupTracker.messages.getFirst().nextMessage());
        }
        return i;
    }

    private GroupTracker getGroupTracker() {
        String freeGroup = freeGroups.poll();
        if (freeGroup == null) {
            return null;
        }

        GroupTracker tracker = allGroupedMessages.get(freeGroup);
        if (tracker == null || tracker.status == GroupStatus.IN_FLIGHT) {
            throw new IllegalStateException(String.format("Polled group %s: %s", freeGroup, tracker));
        }

        tracker.status = GroupStatus.IN_FLIGHT;
        return tracker;
    }

    private CompletableFuture<Void> replenishAvailableGroups() {
        return consumer.receiveAsync().thenApply(polledMessages -> {
            replenishAvailableGroups(polledMessages);
            return null;
        });
    }

    private void replenishAvailableGroups(PolledMessages<O> polledMessages) {
        Map<String, List<MessageTracker>> groupedMessages = groupMessagesByGroupId(polledMessages);
        for (Map.Entry<String, List<MessageTracker>> group : groupedMessages.entrySet()) {
            MessageBatch newBatch = new MessageBatch(group.getValue());
            MutableBoolean isNewGroup = new MutableBoolean(false);
            allGroupedMessages.compute(group.getKey(), (groupId, tracker) -> {
                if (tracker == null) {
                    tracker = new GroupTracker();
                    isNewGroup.setTrue();
                }
                tracker.messages.add(newBatch);
                return tracker;
            });
            totalInFlightMessages.addAndGet(newBatch.count());
            if (isNewGroup.isTrue()) {
                freeGroups.add(group.getKey());
            }
        }
    }

    private Map<String, List<MessageTracker>> groupMessagesByGroupId(PolledMessages<O> polledMessages) {
        Map<String, List<MessageTracker>> groups = new HashMap<>();
        for (PolledMessage<O> polledMessage : polledMessages) {
            MessageTracker messageTracker = new PolledMessageTracker<>(consumer, polledMessage);
            String groupId = messageTracker.getGroupId();
            if (StringUtils.isBlank(groupId)) {
                throw new IllegalStateException("Group id not found for message " + messageTracker.getMessage());
            }
            groups.computeIfAbsent(groupId, list -> new ArrayList<>()).add(messageTracker);
        }
        return groups;
    }

    private boolean hasMaxInFlightMessages() {
        return totalInFlightMessages.get() >= maxInFlightMessages;
    }

    enum GroupStatus {
        // no message for this group is in active processing by the consumer
        FREE,

        // at least one message for this group is in active processing by the consumer
        IN_FLIGHT
    }

    static class GroupTracker {
        GroupStatus status = GroupStatus.FREE;

        LinkedList<MessageBatch> messages = new LinkedList<>();
    }

    @AllArgsConstructor
    private class GroupedMessageTracker implements MessageTracker {
        private final MessageTracker messageTracker;

        @Override
        public Message getMessage() {
            return messageTracker.getMessage();
        }

        @Override
        public void onConsumed(MessageConsumptionStatus status) {
            messageTracker.onConsumed(status);
            String groupId = getGroupId();
            free(groupId, status);
        }

        private void free(String groupId, MessageConsumptionStatus status) {
            MutableBoolean isRemaining = new MutableBoolean(false);
            allGroupedMessages.compute(groupId, (gId, tracker) -> {
                if (tracker == null || tracker.status == GroupStatus.FREE) {
                    throw new IllegalStateException(String.format("Tried to free group %s: %s", gId, tracker));
                }
                var messages = tracker.messages;
                if (!messages.isEmpty() && messages.getFirst().remaining() == 0) {
                    messages.removeFirst();
                }
                if (!messages.isEmpty()) {
                    tracker.status = GroupStatus.FREE;
                    isRemaining.setTrue();
                    return tracker;
                } else {
                    return null;
                }
            });
            totalInFlightMessages.decrementAndGet();
            if (isRemaining.isTrue()) {
                freeGroups.addFirst(groupId);
            }
        }
    }
}

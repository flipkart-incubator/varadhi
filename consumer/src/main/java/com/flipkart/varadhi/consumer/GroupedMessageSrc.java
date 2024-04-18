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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class GroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final ConcurrentHashMap<String, GroupTracker> allGroupedMessages = new ConcurrentHashMap<>();
    private final BlockingDeque<String> freeGroups = new LinkedBlockingDeque<>();
    private final AtomicLong totalInFlightMessages = new AtomicLong(0);
    private final long maxInFlightMessages = 100; // todo: make configurable

    private final Consumer<O> consumer;

    @Override
    public CompletableFuture<Integer> nextMessages(MessageTracker[] messages) {
        if (hasMaxInFlightMessages()) {
            return replenishAvailableGroups().thenApply(v -> nextMessagesInternal(messages));
        }
        return CompletableFuture.supplyAsync(() -> nextMessagesInternal(messages));
    }

    private int nextMessagesInternal(MessageTracker[] messages) {
        GroupTracker groupTracker = getGroupTracker();
        if (null == groupTracker) {
            return 0;
        }

        MessageTracker messageTracker = groupTracker.messages.getFirst().nextMessage();
        messages[0] = new GroupedMessageTracker(messageTracker);
        return 1;
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
        FREE,
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
                // todo: update group consumption status in tracker?
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

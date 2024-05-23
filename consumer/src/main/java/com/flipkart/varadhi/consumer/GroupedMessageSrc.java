package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Message source that maintains ordering among messages of the same groupId.
 */
@RequiredArgsConstructor
@Slf4j
public class GroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final Context context;
    private final ConcurrentHashMap<String, GroupTracker> allGroupedMessages = new ConcurrentHashMap<>();

    // I need a concurrent queue but with the future based api.
    private final ConcurrentLinkedDeque<String> freeGroups = new ConcurrentLinkedDeque<>();

    private final Consumer<O> consumer;

    /**
     * Used to limit the message buffering. Will be driven via consumer configuration.
     */
    private final long maxUnAckedMessages;

    /**
     * Maintains the count of total messages read from the consumer so far.
     * Required for watermark checks, for when this value runs low we can fetch more messages from the consumer.
     * Counter gets decremented when the message is committed/consumed.
     */
    private final AtomicLong totalUnAckedMessages = new AtomicLong(0);

    // Internal states to manage async state

    /**
     * flag to indicate whether a task to fetch messages from consumer is ongoing.
     */
    private final AtomicBoolean pendingAsyncFetch = new AtomicBoolean(false);

    /**
     * holder to keep the incomplete future object while waiting for new messages or groups to get freed up.
     */
    private final AtomicReference<NextMsgsRequest> pendingRequest = new AtomicReference<>();

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
        int count = nextMessagesInternal(messages);
        if (count > 0) {
            return CompletableFuture.completedFuture(count);
        }

        NextMsgsRequest request = new NextMsgsRequest(new CompletableFuture<>(), messages);
        if (!pendingRequest.compareAndSet(null, request)) {
            throw new IllegalStateException(
                    "nextMessages method is not supposed to be called concurrently. There seems to be a pending nextMessage call");
        }

        // incomplete result is saved. trigger new message fetch.
        optionallyFetchNewMessages();

        // double check, if any free group is available now.
        if (isFreeGroupPresent()) {
            tryCompletePendingRequest();
        }

        return request.result;
    }

    private void tryCompletePendingRequest() {
        NextMsgsRequest request;
        if ((request = pendingRequest.getAndSet(null)) != null) {
            request.result.complete(nextMessagesInternal(request.messages));
        }
    }

    private void optionallyFetchNewMessages() {
        if (!isMaxUnAckedMessagesBreached() && pendingAsyncFetch.compareAndSet(false, true)) {
            // there is more room for new messages. We can initiate a new fetch request, as none is ongoing.
            consumer.receiveAsync().whenComplete((polledMessages, ex) -> {
                if (ex != null) {
                    context.getExecutor().execute(() -> {
                        replenishAvailableGroups(polledMessages);
                        pendingAsyncFetch.set(false);
                    });
                } else {
                    log.error("Error while fetching messages from consumer", ex);
                    throw new IllegalStateException(
                            "should be unreachable. consumer.receiveAsync() should not throw exception.");
                }
            });
        }
    }

    private int nextMessagesInternal(MessageTracker[] messages) {
        int i = 0;
        GroupTracker groupTracker;
        while (i < messages.length && (groupTracker = pollFreeGroup()) != null) {
            messages[i++] = new GroupedMessageTracker(groupTracker.messages.getFirst().nextMessage());
        }
        return i;
    }

    boolean isFreeGroupPresent() {
        return !freeGroups.isEmpty();
    }

    private GroupTracker pollFreeGroup() {
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
            totalUnAckedMessages.addAndGet(newBatch.count());
            if (isNewGroup.isTrue()) {
                freeGroups.add(group.getKey());
                tryCompletePendingRequest();
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

    boolean isMaxUnAckedMessagesBreached() {
        return totalUnAckedMessages.get() >= maxUnAckedMessages;
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
                while (!messages.isEmpty() && messages.getFirst().remaining() == 0) {
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
            totalUnAckedMessages.decrementAndGet();
            if (isRemaining.isTrue()) {
                freeGroups.addFirst(groupId);
                tryCompletePendingRequest();
            }
        }
    }

    record NextMsgsRequest(CompletableFuture<Integer> result, MessageTracker[] messages) {
    }
}

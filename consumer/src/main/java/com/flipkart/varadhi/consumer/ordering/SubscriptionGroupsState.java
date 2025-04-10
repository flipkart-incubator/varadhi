package com.flipkart.varadhi.consumer.ordering;

import com.flipkart.varadhi.consumer.MessageTracker;
import com.flipkart.varadhi.entities.InternalQueueType;
import jakarta.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

public interface SubscriptionGroupsState {
    @Nullable
    GroupPointer getPointer(String groupId);

    /**
     * fetch the pointers for the given messages and populate the pointers array accordingly.
     *
     * @param messages
     * @param pointers
     * @param count
     */
    void populatePointers(MessageTracker[] messages, GroupPointer[] pointers, int count);

    CompletableFuture<Void> messageTransitioned(
        String groupId,
        InternalQueueType consumedQueue,
        MessagePointer consumedFrom,
        InternalQueueType producedQueue,
        MessagePointer producedTo
    );

    CompletableFuture<Void> messageConsumed(
        String groupId,
        InternalQueueType consumedQueue,
        MessagePointer consumedFrom
    );
}

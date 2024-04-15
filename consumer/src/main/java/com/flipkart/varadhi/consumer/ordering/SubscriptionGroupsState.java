package com.flipkart.varadhi.consumer.ordering;

import com.flipkart.varadhi.consumer.MessageTracker;
import jakarta.annotation.Nullable;

public interface SubscriptionGroupsState {
    @Nullable
    QueueGroupPointer getPointer(String groupId);

    /**
     * fetch the pointers for the given messages and populate the pointers array accordingly.
     *
     * @param messages
     * @param pointers
     * @param count
     */
    void populatePointers(MessageTracker[] messages, QueueGroupPointer[] pointers, int count);
}

package com.flipkart.varadhi.consumer.ordering;

import com.flipkart.varadhi.consumer.InternalQueueType;

public class GroupPointer {
    QueueGroupPointer main;
    QueueGroupPointer[] retry;
    QueueGroupPointer deadLetter;

    public InternalQueueType isFailed() {
        for (int i = 0; i < retry.length; i++) {
            QueueGroupPointer pointer = retry[i];
            if (pointer != null && pointer.hasLag()) {
                return InternalQueueType.retryType(i + 1);
            }
        }
        if (deadLetter != null && deadLetter.hasLag()) {
            return InternalQueueType.deadLetterType();
        }
        return null;
    }
}

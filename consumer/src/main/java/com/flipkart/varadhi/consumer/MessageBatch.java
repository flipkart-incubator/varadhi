package com.flipkart.varadhi.consumer;

import java.util.List;

public class MessageBatch {
    private final List<MessageTracker> messages;
    private int offset;

    public MessageBatch(List<MessageTracker> messages) {
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("Creating message batch without any messages");
        }
        this.messages = messages;
        this.offset = 0;
    }

    MessageTracker nextMessage() {
        if (offset < messages.size()) {
            MessageTracker messageTracker = messages.get(offset);
            messages.set(offset++, null);
            return messageTracker;
        }
        throw new IllegalStateException("End of batch reached, nothing to consume");
    }

    public int remaining() {
        return messages.size() - offset;
    }

    public int count() {
        return messages.size();
    }
}

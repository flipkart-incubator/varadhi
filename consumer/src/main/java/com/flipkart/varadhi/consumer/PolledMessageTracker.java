package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;

/**
 * Message tracking implementation for PolledMessage type.
 */
public class PolledMessageTracker<O extends Offset> implements MessageTracker {
    private final PolledMessage<O> message;
    private final Consumer<O> committer;

    public PolledMessageTracker(Consumer<O> committer, PolledMessage<O> message) {
        this.message = message;
        this.committer = committer;
    }

    @Override
    public Message getMessage() {
        return message;
    }

    @Override
    public void onConsumed(MessageConsumptionStatus status) {
        committer.commitIndividualAsync(message);
    }
}

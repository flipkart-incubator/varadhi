package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;

public class UnGroupedMessageTracker<O extends Offset> implements MessageTracker {
    private final PolledMessage<O> message;
    private final Consumer<O> committer;

    public UnGroupedMessageTracker(Consumer<O> committer, PolledMessage<O> message) {
        this.message = message;
        this.committer = committer;
    }

    @Override
    public Message getMessage() {
        return new UnGroupedMessageSrc.PolledMessageWrapper<>(message);
    }

    @Override
    public void onConsumed(MessageConsumptionStatus status) {
        if (status == MessageConsumptionStatus.SENT || status == MessageConsumptionStatus.FILTERED) {
            committer.commitIndividualAsync(message);
        }
    }
}

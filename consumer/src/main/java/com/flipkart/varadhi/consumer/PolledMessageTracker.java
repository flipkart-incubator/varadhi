package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.ProducerMessage;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.google.common.collect.ArrayListMultimap;

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
        return new PolledMessageWrapper<>(message);
    }

    @Override
    public void onConsumed(MessageConsumptionStatus status) {
        committer.commitIndividualAsync(message);
    }

    // TODO(aayush): come up with better modeling of message and message with offset
    static class PolledMessageWrapper<O extends Offset> extends ProducerMessage {
        PolledMessage<O> polledMessage;
        // keeping headers as properties and outside the payload

        public PolledMessageWrapper(PolledMessage<O> polledMessage) {
            super(polledMessage.getPayload(), ArrayListMultimap.create());
            this.polledMessage = polledMessage;
        }
    }
}

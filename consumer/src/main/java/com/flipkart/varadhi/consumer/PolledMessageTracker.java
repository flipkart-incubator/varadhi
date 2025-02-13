package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;

import java.util.function.Function;

/**
 * Message tracking implementation for PolledMessage type.
 */
public class PolledMessageTracker<O extends Offset> implements MessageTracker {
    private final PolledMessage<O> message;
    private final Consumer<O> committer;
    private final Function<InternalQueueType, ConsumerMetrics.Tracker> metricTracker;
    private ConsumerMetrics.Tracker tracker;

    public PolledMessageTracker(
        Consumer<O> committer,
        PolledMessage<O> message,
        Function<InternalQueueType, ConsumerMetrics.Tracker> metricTracker
    ) {
        this.message = message;
        this.committer = committer;
        this.metricTracker = metricTracker;
    }

    @Override
    public PolledMessage<O> getMessage() {
        return message;
    }

    @Override
    public void onConsumeStart(InternalQueueType queueType) {
        tracker = metricTracker.apply(queueType);
    }

    @Override
    public void onConsumed(MessageConsumptionStatus status) {
        if (tracker != null) {
            tracker.end(status);
        }
        committer.commitIndividualAsync(message);
    }
}

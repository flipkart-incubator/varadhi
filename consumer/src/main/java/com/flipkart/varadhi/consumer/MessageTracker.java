package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.PolledMessage;

public interface MessageTracker {

    PolledMessage<? extends Offset> getMessage();

    default String getGroupId() {
        Message msg = getMessage();
        return msg == null ? null : msg.getGroupId();
    }

    void onConsumeStart(InternalQueueType queueType);

    void onConsumed(MessageConsumptionStatus status);
}

package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;

public interface MessageTracker {

    Message getMessage();

    default String getGroupId() {
        Message msg = getMessage();
        return msg == null ? null : msg.getGroupId();
    }

    void onConsumed(MessageConsumptionStatus status);
}

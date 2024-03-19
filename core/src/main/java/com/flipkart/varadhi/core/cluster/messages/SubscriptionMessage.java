package com.flipkart.varadhi.core.cluster.messages;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionMessage extends ClusterMessage {
    private final SubscriptionOperation.OpData operation;

    public SubscriptionMessage(SubscriptionOperation.OpData operation) {
        super();
        this.operation = operation;
    }

    public SubscriptionMessage(String id, long timestamp, SubscriptionOperation.OpData operation) {
        super(id, timestamp);
        this.operation = operation;
    }
}

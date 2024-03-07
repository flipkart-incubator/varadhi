package com.flipkart.varadhi.core.cluster.messages;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionMessage extends ClusterMessage {
    private final String subscriptionId;
    private final SubscriptionOperation operation;

    public SubscriptionMessage(SubscriptionOperation operation) {
        super();
        this.subscriptionId = operation.getSubscriptionId();
        this.operation = operation;
    }
}

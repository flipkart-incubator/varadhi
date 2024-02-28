package com.flipkart.varadhi.cluster;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionMessage extends ClusterMessage {
    private final String subscriptionId;

    public SubscriptionMessage(String subscriptionId) {
        super();
        this.subscriptionId = subscriptionId;
    }

}

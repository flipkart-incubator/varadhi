package com.flipkart.varadhi.cluster;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class StartSubscription extends SubscriptionMessage {
    public StartSubscription(String subscriptionId) {
        super(subscriptionId);
    }
}

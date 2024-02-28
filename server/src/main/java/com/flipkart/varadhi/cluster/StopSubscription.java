package com.flipkart.varadhi.cluster;


import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class StopSubscription extends SubscriptionMessage {
    public StopSubscription(String subscriptionId) {
        super(subscriptionId);
    }
}

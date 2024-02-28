package com.flipkart.varadhi.cluster;


import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class DeleteSubscription extends SubscriptionMessage {
    public DeleteSubscription(String subscriptionId) {
        super(subscriptionId);
    }
}

package com.flipkart.varadhi.cluster;


import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateSubscription extends SubscriptionMessage {
    public CreateSubscription(String subscriptionId) {
        super(subscriptionId);
    }
}

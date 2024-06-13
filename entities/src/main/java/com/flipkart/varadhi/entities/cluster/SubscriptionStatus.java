package com.flipkart.varadhi.entities.cluster;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SubscriptionStatus {
    private String subscriptionId;
    private SubscriptionState state;

    public boolean canDelete() {
        return state == SubscriptionState.STOPPED;
    }
}

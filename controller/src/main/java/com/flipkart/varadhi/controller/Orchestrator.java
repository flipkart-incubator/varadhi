package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.messages.StartSubscription;
import com.flipkart.varadhi.core.cluster.messages.StopSubscription;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class Orchestrator {

    public CompletableFuture<Void> handleSubscriptionMessage(SubscriptionMessage subscriptionMessage) {
        if (subscriptionMessage instanceof StartSubscription) {
            return startSubscription((StartSubscription) subscriptionMessage);
        } else if (subscriptionMessage instanceof StopSubscription) {
            return stopSubscription((StopSubscription) subscriptionMessage);
        }
        return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown SubscriptionMessage type"));
    }


    private CompletableFuture<Void> startSubscription(StartSubscription startSubscription) {
        log.info("Starting subscription for {}", startSubscription.getSubscriptionId());
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> stopSubscription(StopSubscription stopSubscription) {
        log.info("Stopping subscription for {}", stopSubscription.getSubscriptionId());
        return CompletableFuture.completedFuture(null);
    }
}


package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.ConsumerState;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.ConsumptionFailurePolicy;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.TopicPartitions;

import java.util.concurrent.CompletableFuture;

public class ConsumersManagerImpl implements ConsumersManager {
    private final ConsumerInfo consumerInfo;

    public ConsumersManagerImpl(ConsumerInfo consumerInfo) {
        this.consumerInfo = consumerInfo;
    }

    @Override
    public CompletableFuture<Void> startSubscription(
            String project, String subscription, int shardId, StorageSubscription<StorageTopic> storageSubscription,
            boolean grouped, Endpoint endpoint, ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    ) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopSubscription(String subscription, int shardId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void pauseSubscription(String subscription, int shardId) {

    }

    @Override
    public void resumeSubscription(String subscription, int shardId) {

    }

    @Override
    public ConsumerState getConsumerState(String subscription, int shardId) {
        return null;
    }

    @Override
    public ConsumerInfo getInfo() {
        return consumerInfo;
    }
}

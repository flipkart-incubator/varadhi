package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.ConsumerState;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.ConsumptionFailurePolicy;
import com.flipkart.varadhi.consumer.VaradhiConsumer;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumersManagerImpl implements ConsumersManager {
    private final ConsumerInfo consumerInfo;
    private final Map<ShardId, ConsumerHolder> consumers = new ConcurrentHashMap<>();

    public ConsumersManagerImpl(ConsumerInfo consumerInfo) {
        this.consumerInfo = consumerInfo;
    }

    @Override
    public CompletableFuture<Void> startSubscription(
            String project, String subscription, int shardId, StorageSubscription<StorageTopic> storageSubscription,
            boolean grouped, Endpoint endpoint, ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    ) {
        ShardId id = new ShardId(subscription, shardId);
        ConsumerHolder prev = consumers.putIfAbsent(id, new ConsumerHolder());
        if (prev != null) {
            throw new IllegalArgumentException("Consumer already exists for " + id);
        }
        ConsumerHolder newConsumer = consumers.get(id);
        newConsumer.consumer = null;

        return null;
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

    record ShardId(String subscription, int shardId) {
    }

    static class ConsumerHolder {
        private VaradhiConsumer consumer;
    }
}

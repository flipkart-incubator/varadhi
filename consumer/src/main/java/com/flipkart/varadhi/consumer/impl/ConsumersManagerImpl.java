package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.concurrent.CustomThread;
import com.flipkart.varadhi.consumer.concurrent.EventExecutor;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.ProducerFactory;

import java.net.http.HttpClient;
import java.util.Map;
import java.util.concurrent.*;

public class ConsumersManagerImpl implements ConsumersManager {

    private final ConsumerEnvironment env;
    private final ScheduledExecutorService scheduler;
    private final EventExecutor executor;

    private final Map<ShardId, ConsumerHolder> consumers = new ConcurrentHashMap<>();

    public ConsumersManagerImpl(
            ProducerFactory<StorageTopic> producerFactory, ConsumerFactory<StorageTopic, Offset> consumerFactory
    ) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.executor = new EventExecutor(this.scheduler, CustomThread::new, new LinkedBlockingQueue<>());
        this.env = new ConsumerEnvironment(producerFactory, consumerFactory, HttpClient.newHttpClient());
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
        newConsumer.consumer =
                new VaradhiConsumerImpl(buildEnv(), project, subscription, shardId, storageSubscription, grouped,
                        endpoint, consumptionPolicy, failurePolicy, new Context(executor), scheduler
                );

        return CompletableFuture.supplyAsync(() -> {
            newConsumer.consumer.start();
            return null;
        });
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

    record ShardId(String subscription, int shardId) {
    }

    static class ConsumerHolder {
        private VaradhiConsumer consumer;
    }

    ConsumerEnvironment buildEnv() {
        return new ConsumerEnvironment(
                producerFactory,
                consumerFactory,
                httpClient
        );
    }
}

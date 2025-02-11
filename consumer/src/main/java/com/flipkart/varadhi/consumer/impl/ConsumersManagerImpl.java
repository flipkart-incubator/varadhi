package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.concurrent.CustomThread;
import com.flipkart.varadhi.consumer.concurrent.EventExecutor;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.ConsumerState;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

@Slf4j
public class ConsumersManagerImpl implements ConsumersManager {

    private final ConsumerEnvironment env;
    private final ScheduledExecutorService scheduler;
    private final EventExecutor executor;

    private final Map<ShardId, ConsumerHolder> consumers = new ConcurrentHashMap<>();

    public ConsumersManagerImpl(
        ProducerFactory<StorageTopic> producerFactory,
        ConsumerFactory<StorageTopic, Offset> consumerFactory,
        MeterRegistry meterRegistry
    ) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.executor = new EventExecutor(this.scheduler, CustomThread::new, new LinkedBlockingQueue<>());
        this.env = new ConsumerEnvironment(producerFactory, consumerFactory, HttpClient.newHttpClient(), meterRegistry);
    }


    @Override
    public CompletableFuture<Void> startSubscription(
        String project,
        String subscription,
        int shardId,
        StorageSubscription<StorageTopic> storageSubscription,
        boolean grouped,
        Endpoint endpoint,
        ConsumptionPolicy consumptionPolicy,
        ConsumptionFailurePolicy failurePolicy,
        TopicCapacityPolicy capacityPolicy
    ) {
        ShardId id = new ShardId(subscription, shardId);
        ConsumerHolder prev = consumers.putIfAbsent(id, new ConsumerHolder());
        if (prev != null) {
            throw new IllegalArgumentException("Consumer already exists for " + id);
        }
        ConsumerHolder newConsumer = consumers.get(id);
        newConsumer.consumer = new VaradhiConsumerImpl(
            env,
            project,
            subscription,
            shardId,
            storageSubscription,
            grouped,
            endpoint,
            consumptionPolicy,
            failurePolicy,
            new Context(executor),
            scheduler,
            (s, sid, iqs) -> new ConsumerMetrics(env.getMeterRegistry(), s, sid, iqs)
        );
        newConsumer.capacityPolicy = capacityPolicy;

        return CompletableFuture.supplyAsync(() -> {
            log.info("Consumer starting for {}/{}", subscription, id);
            newConsumer.consumer.connect();
            log.info("Consumer connected for {}/{}", subscription, id);
            newConsumer.consumer.start();
            log.info("Consumption started for {}/{}", subscription, id);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> stopSubscription(String subscription, int shardId) {
        ShardId id = new ShardId(subscription, shardId);
        ConsumerHolder holder = consumers.get(id);

        if (holder == null) {
            throw new IllegalArgumentException("Consumer not found for " + id);
        }

        return CompletableFuture.runAsync(() -> {
            try {
                log.info("Consumer stopping for {}/{}", subscription, id);
                holder.consumer.close();
                log.info("Consumer sopped for {}/{}", subscription, id);
            } finally {
                consumers.remove(id, holder);
            }
        });
    }

    @Override
    public void pauseSubscription(String subscription, int shardId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void resumeSubscription(String subscription, int shardId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Optional<ConsumerState> getConsumerState(String subscription, int shardId) {
        return Optional.ofNullable(consumers.get(new ShardId(subscription, shardId)))
                       .map(holder -> holder.consumer.getState());
    }

    @Override
    public Iterable<Info> getConsumersInfo() {
        return () -> consumers.values()
                              .stream()
                              .map(
                                  holder -> new Info(
                                      holder.consumer.getSubscriptionName(),
                                      holder.consumer.getShardId(),
                                      holder.consumer.getState(),
                                      holder.capacityPolicy
                                  )
                              )
                              .iterator();
    }

    static class ConsumerHolder {
        private VaradhiConsumer consumer;
        private TopicCapacityPolicy capacityPolicy;
    }
}

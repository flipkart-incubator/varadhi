package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.delivery.DeliveryResponse;
import com.flipkart.varadhi.consumer.delivery.MessageDelivery;
import com.flipkart.varadhi.consumer.processing.ProcessingLoop;
import com.flipkart.varadhi.consumer.processing.UngroupedProcessingLoop;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.ConsumerState;
import com.flipkart.varadhi.spi.services.Consumer;
import com.google.common.base.Ticker;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class VaradhiConsumerImpl implements VaradhiConsumer {

    private final ConsumerEnvironment env;
    private final String project;
    private final String subscriptionName;
    private final int shardId;
    private final StorageSubscription<StorageTopic> storageSubscription;
    private final boolean grouped;
    private final Endpoint endpoint;
    private final ConsumptionPolicy consumptionPolicy;
    private final ConsumptionFailurePolicy failurePolicy;

    @Getter
    private final Context context;
    private final ScheduledExecutorService scheduler;

    private final ConsumerMetricsBuilder metricsProvider;

    private MessageDelivery deliveryClient;
    private ConcurrencyControl<ProcessingLoop.DeliveryResult> concurrencyControl;
    private SlidingWindowThresholdProvider dynamicThreshold;
    private SlidingWindowThrottler<DeliveryResponse> throttler;
    private ProcessingLoop processingLoop;
    private ConsumerMetrics metrics;


    //todo: any app config

    /**
     * consumer object required storage topic
     */
    private final Map<InternalQueueType, ConsumerHolder> internalConsumers = new HashMap<>();

    /**
     * producer objects for required storage topics
     */
    private final Map<InternalQueueType, FailedMsgProducer> internalProducers = new HashMap<>();

    /*
        secured by synchronized block
     */
    private boolean connected = false;

    /*
        It is fetched by the processing loop & various places to know if the consumer is to be stopped.
     */
    private volatile boolean stopRequested = false;

    /*
        can be updated by the processing loop concurrently
     */
    private volatile ConsumerState state = ConsumerState.CONSUMING;

    @Override
    public String getSubscriptionName() {
        return subscriptionName;
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public ConsumerState getState() {
        return state;
    }

    @Override
    public synchronized void connect() {
        try {
            connectUnsafe();
        } catch (Throwable t) {
            log.error("Error initializing consumer", t);
            state = ConsumerState.ERRORED;
            throw t;
        }
    }

    /**
     * Will throw if anything goes wrong during initialization.
     */
    private synchronized void connectUnsafe() {
        if (connected) {
            throw new IllegalStateException();
        }

        InternalQueueType[] iqPriority = getPriority();

        metrics = metricsProvider.build(subscriptionName, shardId, iqPriority);

        internalConsumers.put(
                InternalQueueType.mainType(),
                createConsumerHolder(
                        createConsumer(storageSubscription, InternalQueueType.mainType()),
                        InternalQueueType.mainType()
                )
        );
        for (int r = 1; r <= failurePolicy.getRetryPolicy().getRetryAttempts(); ++r) {
            StorageSubscription<StorageTopic> retrySubscription = failurePolicy.getRetrySubscription()
                    .getSubscriptionForRetry(r).getSubscriptionForConsume();
            // TODO: the retry delay should be configurable.
            internalConsumers.put(
                    InternalQueueType.retryType(r),
                    createConsumerHolder(
                            createDelayedConsumer(
                                    createConsumer(retrySubscription, InternalQueueType.retryType(r)),
                                    5000
                            ),
                            InternalQueueType.retryType(r)
                    )
            );
        }

        for (int r = 1; r <= failurePolicy.getRetryPolicy().getRetryAttempts(); ++r) {
            internalProducers.put(
                    InternalQueueType.retryType(r),
                    createFailedMsgProducer(
                            failurePolicy.getRetrySubscription().getSubscriptionForRetry(r).getTopicForProduce()
                    )
            );
        }
        internalProducers.put(
                InternalQueueType.deadLetterType(),
                createFailedMsgProducer(failurePolicy.getDeadLetterSubscription().getTopicForProduce())
        );

        concurrencyControl =
                new ConcurrencyControlImpl<>(context, consumptionPolicy.getMaxParallelism(), iqPriority);

        dynamicThreshold = new SlidingWindowThresholdProvider(
                scheduler, Ticker.systemTicker(), 2_000, 1_000,
                consumptionPolicy.getMaxErrorThreshold()
        );
        throttler = new SlidingWindowThrottler<>(scheduler, Ticker.systemTicker(), 1, 1_000, 10, getPriority());

        // TODO: currently throttler is hooked into thresholdProvider externally. Because of that the processing loop now requires for instance.
        // One to mark the delivery rate and the other to acquire the quota from throttler. If throttler was a wrapper on the
        // dynamic threshold then old throttler instance would have required. Maybe add a class to wrap these 2 together and then pass that to the processing loop.
        dynamicThreshold.addListener(newThreshold -> {
            log.info("threshold changed to : {}", newThreshold);
            throttler.onThresholdChange(Math.max(newThreshold, 1));
        });

        // Assuming static endpoint for message delivery.
        deliveryClient = MessageDelivery.of(endpoint, env::getHttpClient);

        if (grouped) {
            throw new UnsupportedOperationException("not implemented");
        } else {
            processingLoop =
                    new UngroupedProcessingLoop(
                            context, createMessageSrcSelector(64), concurrencyControl, dynamicThreshold, throttler,
                            deliveryClient, internalProducers, failurePolicy, consumptionPolicy.getMaxInFlightMessages()
                    );
        }

        connected = true;
    }

    @Override
    public synchronized void start() {
        if (!connected || stopRequested) {
            throw new IllegalStateException(
                    "connected: " + connected + ", current state: " + state + ", stopRequested: " + stopRequested);
        }

        state = ConsumerState.CONSUMING;
        startLoop();
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close() {
        if (stopRequested) {
            return;
        }
        log.info("Closing Consumer {}/{}", subscriptionName, shardId);
        stopRequested = true;
        processingLoop.stop();

        // TODO: long blocking is bad. add provision for non-blocking wait & close and provision to drop all pending processing.
        while (true) {
            int count;
            if ((count = processingLoop.getInFlightMessageCount()) > 0) {
                log.info("Waiting for current in-flight messages {} to be processed", count);
            } else {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        Stream.concat(internalConsumers.values().stream(), internalProducers.values().stream())
                .forEach(closeable -> {
                    try {
                        closeable.close();
                    } catch (Exception e) {
                        log.error("Error closing producer/consumer", e);
                    }
                });
        internalConsumers.clear();
        internalProducers.clear();

        if (metrics != null) {
            metrics.close();
        }

        connected = false;
    }

    record ConsumerHolder(Consumer<Offset> consumer, MessageSrc messageSrc) implements Closeable {
        @Override
        public void close() throws IOException {
            consumer.close();
        }
    }

    MessageSrcSelector createMessageSrcSelector(int batchSize) {
        LinkedHashMap<InternalQueueType, MessageSrc> messageSrcs = new LinkedHashMap<>();
        for (InternalQueueType type : getPriority()) {
            messageSrcs.put(type, internalConsumers.get(type).messageSrc);
        }
        return new MessageSrcSelector(context, messageSrcs, batchSize);
    }

    Consumer<Offset> createConsumer(
            StorageSubscription<StorageTopic> storageSubscription, InternalQueueType queueType) {
        String consumerName = String.format("%s/%s/%s/%s", project, subscriptionName, shardId, queueType.toString());
        Consumer<Offset> consumer =
                env.getConsumerFactory()
                        .newConsumer(
                                List.of(storageSubscription.getTopicPartitions()), storageSubscription.getName(),
                                consumerName, Map.of()
                        );
        return consumer;
    }

    Consumer<Offset> createDelayedConsumer(Consumer<Offset> normalConsumer, long delayMs) {
        return new DelayedConsumer<>(normalConsumer, context, delayMs);
    }

    ConsumerHolder createConsumerHolder(Consumer<Offset> consumer, InternalQueueType queueType) {
        // TODO: configurable unacked messages.
        MessageSrc messageSrc =
                grouped ? new GroupedMessageSrc<>(consumer, 1000, metrics) :
                        new UnGroupedMessageSrc<>(queueType, consumer, metrics);
        return new ConsumerHolder(consumer, messageSrc);
    }

    FailedMsgProducer createFailedMsgProducer(StorageTopic topic) {
        return new FailedMsgProducer(env.getProducerFactory().newProducer(topic));
    }

    void startLoop() {
        context.run(processingLoop);
    }

    InternalQueueType[] getPriority() {
        int maxRetryAttempts = failurePolicy.getRetryPolicy().getRetryAttempts();
        InternalQueueType[] priority = new InternalQueueType[1 + maxRetryAttempts];
        priority[0] = InternalQueueType.mainType();
        for (int r = 1; r <= maxRetryAttempts; ++r) {
            priority[r] = InternalQueueType.retryType(r);
        }

        ArrayUtils.reverse(priority);
        return priority;
    }
}

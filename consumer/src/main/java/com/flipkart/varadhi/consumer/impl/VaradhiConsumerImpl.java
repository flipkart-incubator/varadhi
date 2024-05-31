package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.ordering.GroupPointer;
import com.flipkart.varadhi.consumer.ordering.MessagePointer;
import com.flipkart.varadhi.consumer.ordering.SubscriptionGroupsState;
import com.flipkart.varadhi.consumer.push.PushClient;
import com.flipkart.varadhi.consumer.push.PushResponse;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.spi.services.*;
import com.google.common.base.Ticker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class VaradhiConsumerImpl implements VaradhiConsumer {

    private final ConsumerFactory<StorageTopic, Offset> consumerFactory;
    private final ProducerFactory<StorageTopic> producerFactory;
    private final Project project;
    private final String subscriptionName;
    private final String shardName;
    private final TopicPartitions<StorageTopic> topic;
    private final boolean grouped;
    private final Endpoint endpoint;
    private final ConsumptionPolicy consumptionPolicy;
    private final ConsumptionFailurePolicy failurePolicy;
    private final int maxInFlightMessages;

    private final Context context;
    private final ScheduledExecutorService scheduler;
    private final SubscriptionGroupsState subscriptionGroupsState;
    private PushClient pushClient;
    private ConcurrencyControl<PushResponse> concurrencyControl;
    private SlidingWindowThresholdProvider dynamicThreshold;
    private SlidingWindowThrottler<PushResponse> throttler;


    //todo: any app config

    /**
     * consumer object required storage topic
     */
    private final Map<InternalQueueType, ConsumerHolder> internalConsumers = new HashMap<>();

    /**
     * producer objects for required storage topics
     */
    private final Map<InternalQueueType, Producer> internalProducers = new HashMap<>();

    private volatile boolean connected = false;
    private volatile boolean started = false;
    private volatile boolean stopRequested = false;

    // TODO: evaluate var handle atomic operations
    private AtomicInteger inFlightMessages = new AtomicInteger(0);

    InternalQueueType[] getPriority() {
        InternalQueueType[] priority = new InternalQueueType[1 + failurePolicy.getRetryTopic().getMaxRetryCount()];
        priority[0] = InternalQueueType.mainType();
        for (int r = 1; r <= failurePolicy.getRetryTopic().getMaxRetryCount(); ++r) {
            priority[r] = InternalQueueType.retryType(r);
        }

        ArrayUtils.reverse(priority);
        return priority;
    }

    @Override
    public String getSubscriptionName() {
        return subscriptionName;
    }

    @Override
    public String getShardName() {
        return shardName;
    }

    @Override
    public ConsumerState getState() {
        throw new NotImplementedException();
    }

    @Override
    public synchronized void connect() {
        if (connected) {
            throw new IllegalStateException();
        }

        internalConsumers.computeIfAbsent(InternalQueueType.mainType(), type -> createConsumer(topic, "main"));
        for (int r = 1; r <= failurePolicy.getRetryTopic().getMaxRetryCount(); ++r) {
            String role = "retry-" + r;
            TopicPartitions<StorageTopic> retryTopic =
                    TopicPartitions.byTopic(failurePolicy.getRetryTopic().getTopicForRetry(r));
            internalConsumers.computeIfAbsent(
                    InternalQueueType.retryType(r), type -> createConsumer(retryTopic, role));
        }

        for (int r = 1; r <= failurePolicy.getRetryTopic().getMaxRetryCount(); ++r) {
            internalProducers.put(
                    InternalQueueType.retryType(r), createProducer(failurePolicy.getRetryTopic().getTopicForRetry(r)));
        }
        internalProducers.put(InternalQueueType.deadLetterType(), createProducer(failurePolicy.getDeadLetterTopic()));

        concurrencyControl =
                new ConcurrencyControlImpl<>(context, consumptionPolicy.getMaxParallelism(), getPriority());

        dynamicThreshold = new SlidingWindowThresholdProvider(scheduler, Ticker.systemTicker(), 2_000, 1_000,
                consumptionPolicy.getMaxErrorThreshold()
        );
        throttler = new SlidingWindowThrottler<>(scheduler, Ticker.systemTicker(), 1, 1_000, 10, getPriority());
        dynamicThreshold.addListener(newThreshold -> {
            log.debug("threshold changed to : {}", newThreshold);
            throttler.onThresholdChange(Math.max(newThreshold, 1));
        });

        // 1% failure
        pushClient = new PushClient.Flaky(0.01);

        connected = true;
    }

    @Override
    public synchronized void start() {
        if (!connected || started) {
            throw new IllegalStateException();
        }

        initiateConsumptionLoop();
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public synchronized void close() {
        // todo stop consumption loop

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
        connected = false;
    }

    record ConsumerHolder(Consumer<Offset> consumer, MessageSrc messageSrc) implements Closeable {
        @Override
        public void close() throws IOException {
            consumer.close();
        }
    }

    ConsumerHolder createConsumer(TopicPartitions<StorageTopic> partitions, String role) {
        // todo: create proper consumer name
        String consumerName = String.format("%s/%s/%s", project.getName(), subscriptionName, shardName);
        Consumer<Offset> consumer =
                consumerFactory.newConsumer(List.of(partitions), subscriptionName, consumerName, Map.of());
        MessageSrc messageSrc = grouped ? new GroupedMessageSrc<>(consumer, 1000) : new UnGroupedMessageSrc<>(consumer);
        return new ConsumerHolder(consumer, messageSrc);
    }

    Producer createProducer(StorageTopic topic) {
        return producerFactory.newProducer(topic);
    }

    void initiateConsumptionLoop() {
        internalConsumers.forEach((type, consumerHolder) -> {
            ConsumptionLoop loop = new ConsumptionLoop(type, consumerHolder.messageSrc, 64);
            context.execute(loop);
        });
    }

    class ConsumptionLoop implements Context.Task {

        private final InternalQueueType type;
        private final MessageSrc messageSrc;
        private final MessageTracker[] messages;
        private final GroupPointer[] groupPointers;
        private final AtomicBoolean messageFetchInProgress = new AtomicBoolean(false);

        public ConsumptionLoop(
                InternalQueueType type, MessageSrc messageSrc, int maxProcessingBatchSize
        ) {
            this.type = type;
            this.messageSrc = messageSrc;
            this.messages = new MessageTracker[maxProcessingBatchSize];
            this.groupPointers = new GroupPointer[maxProcessingBatchSize];
        }

        @Override
        public Context getContext() {
            return context;
        }

        public void runLoopIfRequired() {
            if (inFlightMessages.get() < maxInFlightMessages && messageFetchInProgress.compareAndSet(false, true)) {
                context.execute(this);
            }
        }

        @Override
        public void run() {
            CompletableFuture<Integer> fetchedFut = messageSrc.nextMessages(messages);
            fetchedFut.whenComplete((fetched, err) -> {
                inFlightMessages.addAndGet(fetched);
                // need to go back to the context. otherwise we might end up using unintended thread.
                context.executeOnContext(() -> onMessagesFetched(fetched));
                messageFetchInProgress.set(false);
                runLoopIfRequired();
            });
        }

        CompletableFuture<PushResponse> pushMessage(MessageTracker msg) {
            return pushClient.push(msg).thenCompose(response -> {
                if (response.success()) {
                    return CompletableFuture.completedFuture(response);
                } else {
                    return throttler.acquire(type, () -> {
                        // acquired the error throttler. now complete the push task
                        return CompletableFuture.completedFuture(response);
                    }, 1);
                }
            });
        }

        // to run on the context
        void onMessagesFetched(int fetched) {
            subscriptionGroupsState.populatePointers(messages, groupPointers, fetched);
            List<Supplier<CompletableFuture<PushResponse>>> forPush = new ArrayList<>();

            for (int i = 0; i < fetched; ++i) {
                MessageTracker message = messages[i];
                GroupPointer groupPointer = groupPointers[i];
                InternalQueueType failedMsgInQueue = groupPointer == null ? null : groupPointer.isFailed();
                if (failedMsgInQueue == null) {
                    // group has not failed
                    forPush.add(() -> pushMessage(message));
                } else {
                    onGroupFailure(failedMsgInQueue, message);
                }
            }

            if (!forPush.isEmpty()) {
                Collection<CompletableFuture<PushResponse>> asyncResponses =
                        concurrencyControl.enqueueTasks(type, forPush);
                // Some of the push will have succeeded, for which we can begin the post processing.
                // For others we start the failure management.
                asyncResponses.forEach(fut -> fut.whenComplete((response, ex) -> {
                    if (response.success()) {
                        onSuccess(response.msg());
                    } else {
                        onPushFailure(response.msg());
                    }
                }));
            }
        }

        void onSuccess(MessageTracker message) {
            MessagePointer consumedFrom = new MessagePointer(1, message.getMessage().getOffset());
            subscriptionGroupsState.messageConsumed(message.getGroupId(), type, consumedFrom).whenComplete((r, e) -> {
                message.onConsumed(MessageConsumptionStatus.SENT);
                inFlightMessages.decrementAndGet();
                runLoopIfRequired();
            });
        }

        void onFailure(InternalQueueType failedMsgInQueue, MessageTracker message, MessageConsumptionStatus status) {
            // failed msgs are present in failedMsgInQueue, so produce this msg there
            CompletableFuture<Offset> asyncProduce =
                    internalProducers.get(failedMsgInQueue).produceAsync(message.getMessage());

            asyncProduce.thenCompose(offset -> {
                // TODO: add all other info.
                MessagePointer consumedFrom = new MessagePointer(1, message.getMessage().getOffset());
                MessagePointer producedTo = new MessagePointer(1, offset);

                return subscriptionGroupsState.messageTransitioned(
                        message.getGroupId(), type, consumedFrom, failedMsgInQueue, producedTo);
            }).whenComplete((r, e) -> {
                message.onConsumed(status);
                inFlightMessages.decrementAndGet();
                runLoopIfRequired();
            });
        }

        void onPushFailure(MessageTracker message) {
            InternalQueueType nextQueue = nextInternalQueue();
            onFailure(nextQueue, message, MessageConsumptionStatus.FAILED);
        }

        void onGroupFailure(InternalQueueType failedMsgInQueue, MessageTracker message) {
            onFailure(failedMsgInQueue, message, MessageConsumptionStatus.GROUP_FAILED);
        }

        InternalQueueType nextInternalQueue() {
            if (type instanceof InternalQueueType.Main) {
                return InternalQueueType.retryType(1);
            } else if (type instanceof InternalQueueType.Retry retryType) {
                if (retryType.getRetryCount() < failurePolicy.getRetryTopic().getMaxRetryCount()) {
                    return InternalQueueType.retryType(retryType.getRetryCount() + 1);
                } else {
                    return InternalQueueType.deadLetterType();
                }
            } else {
                throw new IllegalStateException("Invalid type: " + type);
            }
        }
    }
}

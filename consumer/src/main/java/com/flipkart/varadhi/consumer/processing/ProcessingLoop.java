package com.flipkart.varadhi.consumer.processing;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.delivery.DeliveryResponse;
import com.flipkart.varadhi.consumer.delivery.MessageDelivery;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public abstract class ProcessingLoop implements Context.Task {

    protected volatile boolean stopRequested = false;

    protected final Context context;
    private final MessageSrcSelector msgSrcSelector;
    private final ConcurrencyControl<DeliveryResult> concurrencyControl;
    private final ThresholdProvider.Dynamic throttleThresholdProvider;
    private final Throttler<DeliveryResponse> throttler;
    private final MessageDelivery deliveryClient;
    private final int maxInFlightMessages;

    private final AtomicInteger inFlightMessages = new AtomicInteger(0);
    private final AtomicBoolean iterationInProgress = new AtomicBoolean(false);

    public void stop() {
        stopRequested = true;
    }

    @Override
    public Context getContext() {
        return context;
    }

    public int getInFlightMessageCount() {
        return inFlightMessages.get();
    }

    /**
     * Enqueues the next iteration of the loop onto the context. This is usually called after the previously fetched
     * messages have been passed to the next stages for processing. Since there is no fetch from messageSrcSelector,
     * it should now be possible to enqueue another iteration.
     * <p>
     * Recap on stages:
     * <li>1. Fetch messages from messageSrcSelector</li>
     * <li>2. Messages Squeeze through the CC.</li>
     * <li>3. Messages are delivered to the destination or another topics.</li>
     * </p>
     * <p>
     * There are 2 bottlenecks where messages can buffer uncontrolled. One at the CC layer, because of throttling and
     * second is at the failure processing where produce to RQ/DLQ topic is facing issues.
     * </p>
     * <p>
     * So, it appears that in some cases, the next iteration doesn't need to run immediately.
     * <li>If we have too many in-flight messages, due to above mentioned bottlenecks. </li>
     * <p>
     * So, we skip if we have too many in-flight messages. But we "enable" iterations as soon as we are under
     * (maxInFlightMessages - batchSize).
     *
     * Can be called on any thread.
     *
     * @param currentInFlightMessages
     */
    public void runLoopIfRequired(int currentInFlightMessages) {
        if (currentInFlightMessages <= Math.max(maxInFlightMessages - msgSrcSelector.getBatchSize(), 0)
            && iterationInProgress.compareAndSet(false, true)) {
            log.debug("enqueuing next iteration. inFlightMessages: {}", currentInFlightMessages);
            context.run(this);
        } else {
            log.debug("skipping next iteration. inFlightMessages: {}", currentInFlightMessages);
        }
    }

    /**
     * One iteration of the consumption loop
     */
    @Override
    public void run() {
        assert context.isInContext();

        if (stopRequested) {
            log.info("stop requested. Not polling messages");
            return;
        }

        CompletableFuture<MessageSrcSelector.PolledMessageTrackers> polledFuture = msgSrcSelector.nextMessages();
        polledFuture.whenComplete((polled, err) -> {
            if (err != null) {
                log.error("unexpected error in fetching messages from msgSelector", err);
            }
            assert err == null;

            // need to go back to the context. otherwise we might end up using unintended thread.
            context.runOnContext(() -> {
                inFlightMessages.addAndGet(polled.getSize());
                onMessagesPolled(polled);
                // polled variable is now free to be released.
                polled.recycle();
                iterationInProgress.set(false);
                runLoopIfRequired(inFlightMessages.get());
            });
        });
    }

    protected void onMessagesPolled(MessageSrcSelector.PolledMessageTrackers polled) {
        assert context.isInContext();
    }

    protected Collection<CompletableFuture<DeliveryResult>> deliverMessages(
        InternalQueueType type,
        Iterable<MessageTracker> msg
    ) {
        List<Supplier<CompletableFuture<DeliveryResult>>> forPush = new ArrayList<>();
        for (MessageTracker message : msg) {
            forPush.add(() -> deliver(type, message).thenApply(r -> new DeliveryResult(r, message)));
        }
        return concurrencyControl.enqueueTasks(type, forPush);
    }

    /**
     * Deliver the message to the destination. If the delivery fails, then we wait for the quota from the error
     * throttler.
     */
    private CompletableFuture<DeliveryResponse> deliver(InternalQueueType type, MessageTracker msg) {
        try {
            // msg delivery marks the start of the consumption of the message.
            msg.onConsumeStart(type);

            return deliveryClient.deliver(msg.getMessage()).thenCompose(response -> {
                throttleThresholdProvider.mark();
                log.info(
                    "Delivery attempt was made. queue: {}, message id: {}. status: {}",
                    type,
                    msg.getMessage().getHeader(HeaderUtils.getHeader(MessageHeaders.MSG_ID)),
                    response.statusCode()
                );
                if (response.success()) {
                    return CompletableFuture.completedFuture(response);
                } else {
                    return throttler.acquire(type, () -> {
                        // acquired the error throttler. now complete the push task
                        return CompletableFuture.completedFuture(response);
                    }, 1);
                }
            });
        } catch (Exception e) {
            throttleThresholdProvider.mark();
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Called when a message's processing is finished completely. Delivery Failure or Success.
     * Likely called on the IO thread.
     *
     * @param message
     * @param status
     */
    protected void onComplete(MessageTracker message, MessageConsumptionStatus status) {
        log.info(
            "Message processing complete. message id: {}, status: {}",
            message.getMessage().getMessageId(),
            status
        );
        // all kind of processing finishes here for the message.
        message.onConsumed(status);

        runLoopIfRequired(inFlightMessages.decrementAndGet());
    }

    public record DeliveryResult(DeliveryResponse response, MessageTracker message) {
    }
}

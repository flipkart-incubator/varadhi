package com.flipkart.varadhi.consumer.processing;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.delivery.DeliveryResponse;
import com.flipkart.varadhi.consumer.delivery.MessageDelivery;
import com.flipkart.varadhi.entities.InternalQueueType;
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

    public void runLoopIfRequired(int currentInFlightMessages) {
        // isCCFree() is needed by iteration_body & processing_end(). ifCCFree is false, then skip next_iteration
        // CC.onFree() will trigger next_iteration

        if (currentInFlightMessages < maxInFlightMessages && concurrencyControl.isFree() &&
                iterationInProgress.compareAndSet(false, true)) {
            context.run(this);
        } else {
            concurrencyControl.onFree(this);
        }
    }

    /**
     * One iteration of the consumption loop
     */
    @Override
    public void run() {
        assert context.isInContext();

        if(stopRequested) {
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
            InternalQueueType type, Iterable<MessageTracker> msg
    ) {
        List<Supplier<CompletableFuture<DeliveryResult>>> forPush = new ArrayList<>();
        for (MessageTracker message : msg) {
            forPush.add(() -> deliver(type, message).thenApply(r -> new DeliveryResult(r, message)));
        }
        return concurrencyControl.enqueueTasks(type, forPush);
    }

    private CompletableFuture<DeliveryResponse> deliver(InternalQueueType type, MessageTracker msg) {
        try {
            return deliveryClient.deliver(msg.getMessage()).thenCompose(response -> {
                log.info("Delivered message. status: {}", response.statusCode());
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
            return CompletableFuture.failedFuture(e);
        }
    }

    protected void onComplete(MessageTracker message, MessageConsumptionStatus status) {
        message.onConsumed(status);
        runLoopIfRequired(inFlightMessages.decrementAndGet());
    }

    public record DeliveryResult(DeliveryResponse response, MessageTracker message) {
    }
}

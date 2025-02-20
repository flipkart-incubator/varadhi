package com.flipkart.varadhi.consumer.processing;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.delivery.DeliveryResponse;
import com.flipkart.varadhi.consumer.delivery.MessageDelivery;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class UngroupedProcessingLoop extends ProcessingLoop {

    private final Map<InternalQueueType, FailedMsgProducer> internalProducers;
    private final ConsumptionFailurePolicy failurePolicy;

    public UngroupedProcessingLoop(
            Context context,
            MessageSrcSelector msgSrcSelector, ConcurrencyControl<DeliveryResult> concurrencyControl,
            ThresholdProvider.Dynamic throttleThresholdProvider, Throttler<DeliveryResponse> throttler,
            MessageDelivery deliveryClient, Map<InternalQueueType, FailedMsgProducer> internalProducers,
            ConsumptionFailurePolicy failurePolicy, int maxInFlightMessages
    ) {
        super(
                context, msgSrcSelector, concurrencyControl, throttleThresholdProvider, throttler, deliveryClient,
                maxInFlightMessages
        );
        this.internalProducers = internalProducers;
        this.failurePolicy = failurePolicy;
    }

    @Override
    protected void onMessagesPolled(MessageSrcSelector.PolledMessageTrackers polled) {
        super.onMessagesPolled(polled);

        log.debug("Got {} polled messages to process.", polled.getSize());
        if (polled.getSize() > 0) {
            Collection<CompletableFuture<DeliveryResult>> asyncResponses =
                    deliverMessages(
                            polled.getInternalQueueType(),
                            () -> Arrays.stream(polled.getMessages()).limit(polled.getSize()).iterator()
                    );
            // Some of the push will have succeeded, for which we can begin the post processing.
            // For others we start the failure management.
            asyncResponses.forEach(fut -> fut.whenComplete((response, ex) -> {
                if (response.response().success()) {
                    onComplete(response.message(), MessageConsumptionStatus.SENT);
                } else {
                    onDeliveryFailure(polled.getInternalQueueType(), response.message());
                }
            }));
        }
    }

    void onFailure(
            InternalQueueType type, InternalQueueType failedMsgInQueue, MessageTracker message,
            MessageConsumptionStatus status
    ) {
        // failed msgs are present in failedMsgInQueue, so produce this msg there
        CompletableFuture<Offset> asyncProduce =
                internalProducers.get(failedMsgInQueue).produceAsync(message.getMessage());
        asyncProduce.whenComplete((offset, e) -> {
            log.debug(
                    "Produced failed message to internal queue: {} with offset: {}. msg id: {}", failedMsgInQueue,
                    offset,
                    message.getMessage().getHeader(
                            HeaderUtils.getHeader(StandardHeaders.MSG_ID)
                    )
            );
            onComplete(message, status);
        });
    }

    void onDeliveryFailure(InternalQueueType type, MessageTracker message) {
        InternalQueueType nextQueue = nextInternalQueue(type);
        onFailure(type, nextQueue, message, MessageConsumptionStatus.FAILED);
    }

    InternalQueueType nextInternalQueue(InternalQueueType type) {
        if (type instanceof InternalQueueType.Main) {
            return InternalQueueType.retryType(1);
        } else if (type instanceof InternalQueueType.Retry retryType) {
            if (retryType.getRetryCount() < failurePolicy.getRetryPolicy().getRetryAttempts()) {
                return InternalQueueType.retryType(retryType.getRetryCount() + 1);
            } else {
                return InternalQueueType.deadLetterType();
            }
        } else {
            throw new IllegalStateException("Invalid type: " + type);
        }
    }
}

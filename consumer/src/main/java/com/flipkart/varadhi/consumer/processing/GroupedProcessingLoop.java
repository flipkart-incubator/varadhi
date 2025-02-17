package com.flipkart.varadhi.consumer.processing;

import com.flipkart.varadhi.consumer.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.delivery.DeliveryResponse;
import com.flipkart.varadhi.consumer.delivery.MessageDelivery;
import com.flipkart.varadhi.consumer.ordering.GroupPointer;
import com.flipkart.varadhi.consumer.ordering.MessagePointer;
import com.flipkart.varadhi.consumer.ordering.SubscriptionGroupsState;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GroupedProcessingLoop extends ProcessingLoop {

    private final GroupPointer[] groupPointers;
    private final SubscriptionGroupsState subscriptionGroupsState;
    private final Map<InternalQueueType, FailedMsgProducer> internalProducers;
    private final ConsumptionFailurePolicy failurePolicy;

    public GroupedProcessingLoop(
            Context context,
            MessageSrcSelector msgSrcSelector, ConcurrencyControl<DeliveryResult> concurrencyControl,
            ThresholdProvider.Dynamic throttleThresholdProvider, Throttler<DeliveryResponse> throttler,
            MessageDelivery deliveryClient,
            SubscriptionGroupsState subscriptionGroupsState,
            Map<InternalQueueType, FailedMsgProducer> internalProducers,
            ConsumptionFailurePolicy failurePolicy, int maxInFlightMessages
    ) {
        super(
                context, msgSrcSelector, concurrencyControl, throttleThresholdProvider, throttler, deliveryClient,
                maxInFlightMessages
        );
        this.groupPointers = new GroupPointer[msgSrcSelector.getBatchSize()];
        this.subscriptionGroupsState = subscriptionGroupsState;
        this.internalProducers = internalProducers;
        this.failurePolicy = failurePolicy;
    }

    @Override
    protected void onMessagesPolled(MessageSrcSelector.PolledMessageTrackers polled) {
        super.onMessagesPolled(polled);

        subscriptionGroupsState.populatePointers(polled.getMessages(), groupPointers, polled.getSize());
        List<MessageTracker> forPush = new ArrayList<>();

        for (int i = 0; i < polled.getSize(); ++i) {
            MessageTracker message = polled.getMessages()[i];
            GroupPointer groupPointer = groupPointers[i];
            InternalQueueType failedMsgInQueue = groupPointer == null ? null : groupPointer.isFailed();
            if (failedMsgInQueue == null) {
                // group has not failed
                forPush.add(message);
            } else {
                onGroupFailure(polled.getInternalQueueType(), failedMsgInQueue, message);
            }
        }

        if (!forPush.isEmpty()) {
            Collection<CompletableFuture<DeliveryResult>> asyncResponses =
                    deliverMessages(polled.getInternalQueueType(), forPush);
            // Some of the push will have succeeded, for which we can begin the post processing.
            // For others we start the failure management.
            asyncResponses.forEach(fut -> fut.whenComplete((response, ex) -> {
                if (response.response().success()) {
                    onSuccess(polled.getInternalQueueType(), response.message());
                } else {
                    onPushFailure(polled.getInternalQueueType(), response.message());
                }
            }));
        }
    }

    void onSuccess(InternalQueueType type, MessageTracker message) {
        // TODO: fix the internal topic idx
        MessagePointer consumedFrom = new MessagePointer(1, message.getMessage().getOffset());
        subscriptionGroupsState.messageConsumed(message.getGroupId(), type, consumedFrom).whenComplete((r, e) -> {
            onComplete(message, MessageConsumptionStatus.SENT);
        });
    }

    void onFailure(
            InternalQueueType type, InternalQueueType failedMsgInQueue, MessageTracker message,
            MessageConsumptionStatus status
    ) {
        // failed msgs are present in failedMsgInQueue, so produce this msg there
        CompletableFuture<Offset> asyncProduce =
                internalProducers.get(failedMsgInQueue).produceAsync(message.getMessage());

        asyncProduce.thenCompose(offset -> {
            // TODO: add all other info. fix the internal topic idx
            MessagePointer consumedFrom = new MessagePointer(1, message.getMessage().getOffset());
            MessagePointer producedTo = new MessagePointer(1, offset);

            return subscriptionGroupsState.messageTransitioned(
                    message.getGroupId(), type, consumedFrom, failedMsgInQueue, producedTo);
        }).whenComplete((r, e) -> onComplete(message, status));
    }

    void onPushFailure(InternalQueueType type, MessageTracker message) {
        InternalQueueType nextQueue = nextInternalQueue(type);
        onFailure(type, nextQueue, message, MessageConsumptionStatus.FAILED);
    }

    void onGroupFailure(InternalQueueType type, InternalQueueType failedMsgInQueue, MessageTracker message) {
        onFailure(type, failedMsgInQueue, message, MessageConsumptionStatus.GROUP_FAILED);
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

package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Slf4j
public class DlqService {
    private final ControllerApi controllerClient;
    private final ConsumerClientFactory consumerFactory;

    public DlqService(ControllerApi controllerClient, ConsumerClientFactory consumerFactory) {
        this.controllerClient = controllerClient;
        this.consumerFactory = consumerFactory;
    }

    public CompletableFuture<SubscriptionOperation> unsideline(
            VaradhiSubscription subscription, UnsidelineRequest unsidelineRequest, String requestedBy
    ) {
        if (!subscription.isWellProvisioned()) {
            throw new InvalidOperationForResourceException(
                    "Subscription is in state %s. Unsideline not allowed.".formatted(
                            subscription.getStatus().getState()));
        }
        return controllerClient.unsideline(subscription.getName(), unsidelineRequest, requestedBy);
    }


    public CompletableFuture<Void> getMessages(
            VaradhiSubscription subscription, GetMessagesRequest messagesRequest,
            Consumer<GetMessagesResponse> responseWriter
    ) {
        if (!subscription.isWellProvisioned()) {
            throw new InvalidOperationForResourceException(
                    "Subscription is in state %s. GetMessages not allowed.".formatted(
                            subscription.getStatus().getState()));
        }
        // Get subscription shard's consumer
        // call getMessage() for each shard on respective consumers.
        // Write response back as and when received from each shard.
        // Return CompletedFuture<Void> will complete/end the request.
        return controllerClient.getShardAssignments(subscription.getName())
                .thenCompose(shardAssignments -> CompletableFuture.allOf(
                        shardAssignments.getAssignments().stream().map(assignment -> {
                            ConsumerApi consumer = consumerFactory.getInstance(assignment.getConsumerId());
                            return consumer.getMessages(messagesRequest).whenComplete((messages, t) -> {
                                // write partial response i.e. return messages from single shard or error.
                                GetMessagesResponse shardResponse = GetMessagesResponse.of(assignment.getShardId());
                                if (t == null) {
                                    shardResponse.setMessages(messages);
                                } else {
                                    log.error(
                                            "Failed to getMessages for {}(shard:{}). Error:{}", subscription.getName(),
                                            assignment.getShardId(), t.getMessage()
                                    );
                                    shardResponse.setError(t.getMessage());
                                }
                                log.info("Sending a GetMessage response for {}", assignment);
                                responseWriter.accept(shardResponse);
                            });
                        }).toArray(CompletableFuture[]::new)));
    }
}

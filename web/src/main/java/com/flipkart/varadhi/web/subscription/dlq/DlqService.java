package com.flipkart.varadhi.web.subscription.dlq;

import com.flipkart.varadhi.core.cluster.api.ConsumerApi;
import com.flipkart.varadhi.core.cluster.api.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.api.ControllerApi;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.core.subscription.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.web.entities.DlqMessagesResponse;
import com.flipkart.varadhi.web.entities.DlqPageMarker;
import com.flipkart.varadhi.web.entities.ShardDlqMsgResponseCollector;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.flipkart.varadhi.entities.UnsidelineRequest.UNSPECIFIED_TS;

@Slf4j
public class DlqService {
    private final ControllerApi controllerClient;
    private final ConsumerClientFactory consumerFactory;

    public DlqService(ControllerApi controllerClient, ConsumerClientFactory consumerFactory) {
        this.controllerClient = controllerClient;
        this.consumerFactory = consumerFactory;
    }

    public CompletableFuture<SubscriptionOperation> unsideline(
        VaradhiSubscription subscription,
        UnsidelineRequest unsidelineRequest,
        String requestedBy
    ) {
        if (!subscription.isActive()) {
            throw new InvalidOperationForResourceException(
                "Subscription is in state %s. Unsideline not allowed.".formatted(subscription.getStatus().getState())
            );
        }
        return controllerClient.unsideline(subscription.getName(), unsidelineRequest, requestedBy);
    }


    public CompletableFuture<Void> getMessages(
        VaradhiSubscription subscription,
        long earliestFailedAt,
        DlqPageMarker pageMarkers,
        int limit,
        Consumer<DlqMessagesResponse> recordWriter
    ) {
        if (!subscription.isActive()) {
            throw new InvalidOperationForResourceException(
                "Dlq messages can't be queried in Subscription's current state %s.".formatted(
                    subscription.getStatus().getState()
                )
            );
        }
        // Get subscription shard's consumer
        // call getMessage() for each shard on respective consumers.
        // Write response back as and when received from each shard.
        // Return CompletedFuture<Void> will complete/end the request.
        ShardDlqMsgResponseCollector finalResponse = new ShardDlqMsgResponseCollector();
        boolean isRequestByTimeStamp = earliestFailedAt != UNSPECIFIED_TS;
        return controllerClient.getShardAssignments(subscription.getName()).thenCompose(assignments -> {
            List<CompletableFuture<ShardDlqMessageResponse>> shardFutures = new ArrayList<>();
            for (Assignment a : assignments.getAssignments()) {
                int shardId = a.getShardId();
                if (!isRequestByTimeStamp && !pageMarkers.hasMarker(shardId)) {
                    log.info("Shard {} has no markers, skipping getMessages().", shardId);
                    continue;
                }
                shardFutures.add(
                    getMessagesForShard(
                        isRequestByTimeStamp,
                        a.getConsumerId(),
                        earliestFailedAt,
                        pageMarkers.getShardMarker(shardId),
                        limit
                    ).whenComplete((r, t) -> processShardResponse(shardId, recordWriter, r, t, finalResponse))
                );
            }
            return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]));
        }).whenComplete((v, t) -> recordWriter.accept(finalResponse.toAggregatedResponse(t)));
    }

    private void processShardResponse(
        int shardId,
        Consumer<DlqMessagesResponse> recordWriter,
        ShardDlqMessageResponse r,
        Throwable t,
        ShardDlqMsgResponseCollector finalResponse
    ) {
        if (r != null && !r.getMessages().isEmpty()) {
            log.info(
                "shard {} returned {} messages nextMarker {}.",
                shardId,
                r.getMessages().size(),
                r.getNextPageMarker()
            );
            recordWriter.accept(DlqMessagesResponse.of(r.getMessages()));
        }
        finalResponse.collectShardResponse(shardId, t, r);
    }

    private CompletableFuture<ShardDlqMessageResponse> getMessagesForShard(
        boolean isRequestByTimeStamp,
        String consumerId,
        long earliestFailedAt,
        String shardPageMarker,
        int limit
    ) {
        ConsumerApi consumer = consumerFactory.getInstance(consumerId);
        return isRequestByTimeStamp ?
            consumer.getMessagesByTimestamp(earliestFailedAt, limit) :
            consumer.getMessagesByOffset(shardPageMarker, limit);
    }
}

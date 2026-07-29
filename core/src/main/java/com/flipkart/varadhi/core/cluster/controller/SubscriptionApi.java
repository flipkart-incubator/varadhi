package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;

import java.util.concurrent.CompletableFuture;

/**
 * Controller subscription lifecycle APIs over the {@code controller} cluster route.
 *
 * <p>Server pods call these via {@link ControllerRemoteClient} (request/response).
 * Consumer pods use the same contract to report shard-operation completion ({@link #update},
 * fire-and-forget send).
 *
 * <p>Controller-side implementation: {@code com.flipkart.varadhi.controller.SubscriptionService}.
 */
public interface SubscriptionApi {

    /**
     * Returns the aggregated runtime state of a subscription by fanning out to every assigned
     * consumer shard and merging per-shard {@link SubscriptionState} values.
     *
     * @param subscriptionId subscription name
     * @param requestedBy    identity of the caller (audit)
     * @return merged subscription state; unreachable consumers contribute empty shard state
     */
    CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy);

    /**
     * Starts a stopped subscription: validates it is not already assigned, creates a start
     * {@link SubscriptionOperation}, and enqueues it for execution.
     *
     * @param subscriptionId subscription name
     * @param requestedBy    identity of the caller (audit)
     * @return the created operation (persisted and queued)
     * @throws com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException
     *         if the subscription is already assigned
     */
    CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy);

    /**
     * Stops a running or partially assigned subscription: validates it is not already stopped,
     * creates a stop {@link SubscriptionOperation}, and enqueues it for execution.
     *
     * @param subscriptionId subscription name
     * @param requestedBy    identity of the caller (audit)
     * @return the created operation (persisted and queued)
     * @throws com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException
     *         if the subscription is already stopped
     */
    CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy);

    /**
     * Unsidelines messages for a running subscription: validates the subscription is running
     * successfully, creates an unsideline {@link SubscriptionOperation}, and enqueues it.
     *
     * @param subscriptionId subscription name
     * @param request          unsideline parameters (scope, filters)
     * @param requestedBy      identity of the caller (audit)
     * @return the created operation (persisted and queued)
     * @throws com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException
     *         if unsideline is not allowed in the current subscription state
     */
    CompletableFuture<SubscriptionOperation> unsideline(
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    );

    /**
     * Returns the controller's current shard-to-consumer assignments for a subscription.
     *
     * @param subscriptionId subscription name
     * @return shard assignments as known to the controller assignment manager
     */
    CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId);

    /**
     * Reports shard-operation completion or failure from a consumer back to the controller.
     * Fire-and-forget over the cluster bus ({@code send}, not {@code request}).
     *
     * <p>On the controller, handled inline on the dispatcher thread — keep the callback path cheap.
     *
     * @param subOpId    parent subscription-operation id
     * @param shardOpId  shard-operation id
     * @param state      terminal or intermediate shard-op state
     * @param errorMsg   error detail when {@code state} is errored; otherwise may be a success message
     */
    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);
}

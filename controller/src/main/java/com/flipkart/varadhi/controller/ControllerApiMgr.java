package com.flipkart.varadhi.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.controller.impl.opexecutors.*;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.SYSTEM_IDENTITY;

@Slf4j
public class ControllerApiMgr implements ControllerApi {
    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final MetaStore metaStore;
    private final OperationMgr operationMgr;

    public ControllerApiMgr(
            OperationMgr operationMgr, AssignmentManager assignmentManager, MetaStore metaStore,
            ConsumerClientFactory consumerClientFactory
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.metaStore = metaStore;
        this.operationMgr = operationMgr;
    }

    @Override
    public CompletableFuture<SubscriptionStatus> getSubscriptionStatus(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
                .thenCompose(this::getSubscriptionStatus);
    }

    CompletableFuture<SubscriptionStatus> getSubscriptionStatus(VaradhiSubscription subscription) {
        String subId = subscription.getName();

        return CompletableFuture.supplyAsync(() -> assignmentManager.getSubAssignments(subId))
                .thenCompose(assignments -> {
                    List<CompletableFuture<ShardStatus>> shardFutures = assignments.stream().map(a -> {
                        ConsumerApi consumer = consumerClientFactory.getInstance(a.getConsumerId());
                        return consumer.getShardStatus(subId, a.getShardId());
                    }).toList();

                    return CompletableFuture.allOf(shardFutures.toArray(CompletableFuture[]::new)).thenApply(v -> {
                        List<ShardStatus> shardStatuses = new ArrayList<>();
                        shardFutures.forEach(sf -> shardStatuses.add(sf.join()));
                        return getSubscriptionStatusFromShardStatus(subscription, assignments, shardStatuses);
                    });
                }).exceptionally(t -> {
                    // If not temporary, then alternate needs to be provided to allow recovery from this.
                    throw new IllegalStateException(
                            String.format(
                                    "Failure in getting subscription status, try again after sometime. %s",
                                    t.getMessage()
                            ));
                });
    }

    private SubscriptionStatus getSubscriptionStatusFromShardStatus(
            VaradhiSubscription subscription, List<Assignment> assignments, List<ShardStatus> shardStatuses
    ) {
        SubscriptionState state = SubscriptionState.getFromShardStates(assignments, shardStatuses);
        return new SubscriptionStatus(subscription.getName(), state);
    }

    @Override
    public CompletableFuture<SubscriptionOperation> startSubscription(
            String subscriptionId, String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
                .thenCompose(subscription -> getSubscriptionStatus(subscription).thenApply(ss -> {
                    if (ss.getState() == SubscriptionState.RUNNING || ss.getState() == SubscriptionState.STARTING) {
                        throw new InvalidOperationForResourceException(
                                "Subscription is either already running or starting.");
                    }
                    log.info("Starting the Subscription: {}", subscriptionId);
                    SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
                    operationMgr.createAndEnqueue(
                            operation,
                            new StartOpExecutor(
                                    subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore)
                    );
                    return operation;
                }));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(
            String subscriptionId, String requestedBy
    ) {

        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
                .thenCompose(subscription -> getSubscriptionStatus(subscription).thenApply(ss -> {
                    if (ss.getState() == SubscriptionState.STOPPED || ss.getState() == SubscriptionState.STOPPING) {
                        throw new InvalidOperationForResourceException(
                                "Subscription is either already stopped or stopping.");
                    }
                    log.info("Stopping the Subscription: {}", subscriptionId);
                    SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
                    operationMgr.createAndEnqueue(
                            operation,
                            new StopOpExecutor(
                                    subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore)
                    );
                    return operation;
                }));
    }

    @Override
    public CompletableFuture<Void> update(
            String subOpId, String shardOpId, ShardOperation.State state, String errorMsg
    ) {
        log.info(
                "Received update on shard operation: SubOpId={} ShardOpId={}, State={}, Error={}", subOpId, shardOpId,
                state, errorMsg
        );
        try {
            // Update is getting executed inline on dispatcher thread.
            operationMgr.updateShardOp(subOpId, shardOpId, state, errorMsg);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    /*
     * TODO::It should be possible to abort running unsideline operation
     *  - to stop the subscription.
     *  - to stop unsidelining.
     * Aborting/stopping a running operation is not yet supported/implemented.
     */
    @Override
    public CompletableFuture<SubscriptionOperation> unsideline(
            String subscriptionId, UnsidelineRequest request, String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId)).thenCompose(subscription ->
        getSubscriptionStatus(subscription).thenApply(ss -> {
            if (ss.getState() == SubscriptionState.STOPPED || ss.getState() == SubscriptionState.STOPPING) {
                throw new InvalidOperationForResourceException(
                        String.format("Unsideline not allowed in subscription state %s.", ss.getState()));
            }
            SubscriptionOperation operation = SubscriptionOperation.unsidelineOp(subscriptionId, request, requestedBy);
            operationMgr.createAndEnqueue(
                    operation,
                    new UnsidelinepOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager,
                            metaStore
                    )
            );
            return operation;
        }));
    }

    @Override
    public CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId) {
        return CompletableFuture.completedFuture(
                new ShardAssignments(assignmentManager.getSubAssignments(subscriptionId)));
    }

    public CompletableFuture<String> addConsumerNode(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenApply(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            assignmentManager.addConsumerNode(consumerNode);
            return consumerNode.getConsumerId();
        });
    }

    public CompletableFuture<Void> consumerNodeLeft(String consumerNodeId) {
        log.info("ConsumerNode {} left the cluster.", consumerNodeId);
        return assignmentManager.consumerNodeLeft(consumerNodeId).thenAccept((v) -> {
            List<Assignment> assignments = assignmentManager.getConsumerNodeAssignments(consumerNodeId);
            assignments.forEach(assignment -> {
                log.info("Assignment {} needs to be re-assigned", assignment);
                SubscriptionOperation operation = SubscriptionOperation.reAssignShardOp(assignment, SYSTEM_IDENTITY);
                VaradhiSubscription subscription = metaStore.getSubscription(assignment.getSubscriptionId());
                operationMgr.createAndEnqueue(
                        operation,
                        new ReAssignOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager,
                                metaStore
                        )
                );
            });
        });
    }

    public CompletableFuture<Void> consumerNodeJoined(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenCompose(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            return assignmentManager.consumerNodeJoined(consumerNode);
        });
    }

    private CompletableFuture<ConsumerInfo> getConsumerInfo(String consumerId) {
        ConsumerApi consumer = consumerClientFactory.getInstance(consumerId);
        return consumer.getConsumerInfo();
    }

    public List<Assignment> getAllAssignments() {
        return assignmentManager.getAllAssignments();
    }

    public List<SubscriptionOperation> getPendingSubOps() {
        return operationMgr.getPendingSubOps();
    }

    public void retryOperation(SubscriptionOperation operation) {
        VaradhiSubscription subscription = metaStore.getSubscription(operation.getData().getSubscriptionId());
        OpExecutor<OrderedOperation> executor = getOpExecutor(operation, subscription);
        operationMgr.enqueue(operation, executor);
    }

    private OpExecutor<OrderedOperation> getOpExecutor(
            SubscriptionOperation operation, VaradhiSubscription subscription
    ) {
        //TODO::Better handling needed
        if (operation.getData() instanceof SubscriptionOperation.StartData) {
            return new StartOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else if (operation.getData() instanceof SubscriptionOperation.StopData) {
            return new StopOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else if (operation.getData() instanceof SubscriptionOperation.ReassignShardData) {
            return new ReAssignOpExecutor(
                    subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else if (operation.getData() instanceof SubscriptionOperation.UnsidelineData) {
            return new UnsidelinepOpExecutor(
                    subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else {
            throw new IllegalArgumentException("Can't get OpExecutor for Operation %s.".formatted(operation.getData()));
        }
    }

}

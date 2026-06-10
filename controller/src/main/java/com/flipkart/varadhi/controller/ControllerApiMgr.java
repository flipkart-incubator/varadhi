package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.controller.failover.TopicFailoverOpExecutor;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TopicFailoverTransactions;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.controller.impl.opexecutors.ReAssignOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StartOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StopOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.UnsidelinepOpExecutor;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerApi;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.controller.ControllerConsumerApi;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.ConsumerInfo;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.AssignmentState;
import com.flipkart.varadhi.entities.cluster.ConsumerState;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;
import com.flipkart.varadhi.entities.cluster.failover.FailoverTransitionObject;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverTransition;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.flipkart.varadhi.common.Constants.SYSTEM_IDENTITY;

@Slf4j
public class ControllerApiMgr implements ControllerApi, ControllerConsumerApi {
    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final SubscriptionStore subscriptionStore;
    private final OperationMgr operationMgr;

    /**
     * Optional dependencies wired post-construct (so the existing 4-arg constructor
     * keeps compiling). Topic-failover REST endpoints require all three; the
     * subscription endpoints don't touch them.
     */
    private volatile TopicStore topicStore;
    private volatile TopicFailoverTransactions failoverTxns;
    private volatile Supplier<TopicFailoverOpExecutor> failoverExecutorFactory;

    public ControllerApiMgr(
        OperationMgr operationMgr,
        AssignmentManager assignmentManager,
        SubscriptionStore subscriptionStore,
        ConsumerClientFactory consumerClientFactory
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.subscriptionStore = subscriptionStore;
        this.operationMgr = operationMgr;
    }

    /**
     * Wire in topic-failover dependencies. Called once during controller bootstrap
     * after all collaborators (executor, broadcaster, awaiter registry) have been
     * assembled. Until this is called, the failover REST methods reject with
     * {@code IllegalStateException}.
     */
    public void wireTopicFailover(
        TopicStore topicStore,
        TopicFailoverTransactions failoverTxns,
        Supplier<TopicFailoverOpExecutor> failoverExecutorFactory
    ) {
        this.topicStore = topicStore;
        this.failoverTxns = failoverTxns;
        this.failoverExecutorFactory = failoverExecutorFactory;
    }

    @Override
    public CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> subscriptionStore.get(subscriptionId))
                                .thenCompose(this::getSubscriptionState);
    }

    CompletableFuture<SubscriptionState> getSubscriptionState(VaradhiSubscription subscription) {
        String subId = subscription.getName();
        return CompletableFuture.supplyAsync(() -> assignmentManager.getSubAssignments(subId))
                                .thenCompose(assignments -> {
                                    return getSubscriptionShardsState(subscription, assignments, subId);
                                })
                                .exceptionally(t -> {
                                    // If not temporary, then alternate needs to be provided to allow recovery from this.
                                    throw new IllegalStateException(
                                        String.format(
                                            "Failure in getting subscription status, try again after sometime. %s",
                                            t.getMessage()
                                        )
                                    );
                                });
    }

    private CompletableFuture<SubscriptionState> getSubscriptionShardsState(
        VaradhiSubscription subscription,
        List<Assignment> assignments,
        String subId
    ) {
        var shardFutures = assignments.stream().map(a -> {
            var consumer = consumerClientFactory.getInstance(a.getConsumerId());
            return consumer.getConsumerState(subId, a.getShardId()).handle((state, t) -> {
                if (t != null) {
                    return Optional.<ConsumerState>empty();
                }
                return state;
            });
        }).toList();
        return CompletableFuture.allOf(shardFutures.toArray(CompletableFuture[]::new)).thenApply(v -> {
            List<Optional<ConsumerState>> states = new ArrayList<>();
            shardFutures.forEach(sf -> states.add(sf.join()));
            return getSubscriptionStatusFromShardStatus(subscription, assignments, states);
        });
    }

    private SubscriptionState getSubscriptionStatusFromShardStatus(
        VaradhiSubscription subscription,
        List<Assignment> assignments,
        List<Optional<ConsumerState>> states
    ) {
        List<SubscriptionState> shardStates = new ArrayList<>(subscription.getShards().getShardCount());
        for (int i = 0; i < subscription.getShards().getShardCount(); ++i) {
            shardStates.add(new SubscriptionState(AssignmentState.NOT_ASSIGNED, null));
        }

        for (int i = 0; i < assignments.size(); ++i) {
            Assignment a = assignments.get(i);
            Optional<ConsumerState> state = states.get(i);
            int shardId = a.getShardId();

            shardStates.set(shardId, new SubscriptionState(AssignmentState.ASSIGNED, state.orElse(null)));
        }

        return SubscriptionState.mergeShardStates(shardStates);
    }

    @Override
    public CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> subscriptionStore.get(subscriptionId))
                                .thenCompose(subscription -> getSubscriptionState(subscription).thenApply(ss -> {
                                    if (!AssignmentState.NOT_ASSIGNED.equals(ss.getAssignmentState())) {
                                        throw new InvalidOperationForResourceException(
                                            "Subscription is already assigned and may be running."
                                        );
                                    }
                                    log.info("Starting the Subscription: {}", subscriptionId);
                                    SubscriptionOperation operation = SubscriptionOperation.startOp(
                                        subscriptionId,
                                        requestedBy
                                    );
                                    operationMgr.createAndEnqueue(
                                        operation,
                                        new StartOpExecutor(
                                            subscription,
                                            consumerClientFactory,
                                            operationMgr,
                                            assignmentManager,
                                            subscriptionStore
                                        )
                                    );
                                    return operation;
                                }));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> subscriptionStore.get(subscriptionId))
                                .thenCompose(subscription -> getSubscriptionState(subscription).thenApply(ss -> {
                                    // This means that partially assigned subscriptions can be stopped.
                                    if (AssignmentState.NOT_ASSIGNED.equals(ss.getAssignmentState())) {
                                        throw new InvalidOperationForResourceException(
                                            "Subscription is already stopped."
                                        );
                                    }
                                    log.info("Stopping the Subscription: {}", subscriptionId);
                                    SubscriptionOperation operation = SubscriptionOperation.stopOp(
                                        subscriptionId,
                                        requestedBy
                                    );
                                    operationMgr.createAndEnqueue(
                                        operation,
                                        new StopOpExecutor(
                                            subscription,
                                            consumerClientFactory,
                                            operationMgr,
                                            assignmentManager,
                                            subscriptionStore
                                        )
                                    );
                                    return operation;
                                }));
    }

    @Override
    public CompletableFuture<Void> update(
        String subOpId,
        String shardOpId,
        ShardOperation.State state,
        String errorMsg
    ) {
        log.info(
            "Received update on shard operation: SubOpId={} ShardOpId={}, State={}, Error={}",
            subOpId,
            shardOpId,
            state,
            errorMsg
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
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> subscriptionStore.get(subscriptionId))
                                .thenCompose(subscription -> getSubscriptionState(subscription).thenApply(ss -> {
                                    if (!ss.isRunningSuccessfully()) {
                                        throw new InvalidOperationForResourceException(
                                            String.format("Unsideline not allowed in subscription state %s.", ss)
                                        );
                                    }
                                    SubscriptionOperation operation = SubscriptionOperation.unsidelineOp(
                                        subscriptionId,
                                        request,
                                        requestedBy
                                    );
                                    operationMgr.createAndEnqueue(
                                        operation,
                                        new UnsidelinepOpExecutor(
                                            subscription,
                                            consumerClientFactory,
                                            operationMgr,
                                            assignmentManager,
                                            subscriptionStore
                                        )
                                    );
                                    return operation;
                                }));
    }

    @Override
    public CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId) {
        return CompletableFuture.completedFuture(
            new ShardAssignments(assignmentManager.getSubAssignments(subscriptionId))
        );
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
                VaradhiSubscription subscription = subscriptionStore.get(assignment.getSubscriptionId());
                operationMgr.createAndEnqueue(
                    operation,
                    new ReAssignOpExecutor(
                        subscription,
                        consumerClientFactory,
                        operationMgr,
                        assignmentManager,
                        subscriptionStore
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
        VaradhiSubscription subscription = subscriptionStore.get(operation.getData().getSubscriptionId());
        OpExecutor<OrderedOperation> executor = getOpExecutor(operation, subscription);
        operationMgr.enqueue(operation, executor);
    }

    /* ============== Topic Failover ============== */

    @Override
    public CompletableFuture<TopicFailoverTransition> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> {
            requireFailoverWired();
            VaradhiTopic topic = topicStore.get(topicFqn);
            if (topic == null) {
                throw new ResourceNotFoundException("Topic not found: " + topicFqn);
            }
            if (topicStore.hasFailover(topicFqn)) {
                throw new InvalidOperationForResourceException(
                    "Topic " + topicFqn + " already has an in-flight failover; abort it before starting a new one."
                );
            }
            String targetRegion = request.getToRegion();
            if (targetRegion == null || targetRegion.isBlank()) {
                throw new IllegalArgumentException("toRegion is required");
            }
            if (topic.getProduceTopicForRegion(targetRegion) == null) {
                throw new InvalidOperationForResourceException(
                    "Target region " + targetRegion + " is not configured for topic " + topicFqn
                );
            }

            String sourceRegion = resolveActiveRegion(topic, targetRegion);
            TopicFailoverOperation op = TopicFailoverOperation.create(
                topicFqn,
                sourceRegion,
                targetRegion,
                request.isWaitForReplicationLagToClear(),
                request.isSkipValidation(),
                requestedBy
            );
            FailoverTransitionObject fto = FailoverTransitionObject.forTopic(topicFqn, op.getId());

            failoverTxns.createFailoverWithOp(op, fto);
            log.info("Created topic failover op {} for {} (source={} target={})",
                op.getId(), topicFqn, sourceRegion, targetRegion);

            operationMgr.enqueueTopicFailoverOp(op, failoverExecutorFactory.get());
            return TopicFailoverTransition.from(op);
        });
    }

    @Override
    public CompletableFuture<Optional<TopicFailoverTransition>> getTopicFailover(String topicFqn) {
        return CompletableFuture.supplyAsync(() -> {
            requireFailoverWired();
            Optional<FailoverTransitionObject> fto = topicStore.getFailover(topicFqn);
            if (fto.isEmpty()) {
                return Optional.<TopicFailoverTransition>empty();
            }
            return Optional.of(TopicFailoverTransition.from(loadOp(fto.get().getOperationId())));
        });
    }

    @Override
    public CompletableFuture<TopicFailoverTransition> abortTopicFailover(String topicFqn, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> {
            requireFailoverWired();
            FailoverTransitionObject fto = topicStore.getFailover(topicFqn)
                .orElseThrow(() -> new ResourceNotFoundException("No active failover for topic " + topicFqn));
            TopicFailoverOperation op = loadOp(fto.getOperationId());
            if (!op.getCurrentStage().isAbortable()) {
                throw new InvalidOperationForResourceException(
                    "Failover for " + topicFqn + " is past " + op.getCurrentStage() + " and cannot be aborted."
                );
            }
            op.markFail("Aborted by " + requestedBy);
            // Persist the abort intent; the executor's exceptionallyCompose will pick this
            // up on the next stage transition and run cleanup. We deliberately don't try
            // to interrupt the running CompletableFuture chain — letting it finish the
            // current stage barrier is safer than racing.
            failoverTxns.commitFailure(op, topicFqn);
            return TopicFailoverTransition.from(op);
        });
    }

    /** Forwards a pod-side ack to {@link OperationMgr} for stage-barrier dispatch. */
    public void recordFailoverAck(com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate update) {
        operationMgr.recordFailoverAck(update);
    }

    @Override
    public CompletableFuture<List<TopicFailoverTransition>> listActiveTopicFailovers() {
        return CompletableFuture.supplyAsync(() -> {
            requireFailoverWired();
            List<TopicFailoverOperation> live = operationMgr.getActiveTopicFailoverOps();
            return live.stream().map(TopicFailoverTransition::from).toList();
        });
    }

    private TopicFailoverOperation loadOp(String operationId) {
        TopicFailoverOperation op = operationMgr.getTopicFailoverOp(operationId);
        if (op == null) {
            throw new ResourceNotFoundException("TopicFailoverOperation " + operationId + " not found");
        }
        return op;
    }

    private void requireFailoverWired() {
        if (topicStore == null || failoverTxns == null || failoverExecutorFactory == null) {
            throw new IllegalStateException(
                "Topic failover dependencies are not wired; ControllerApiMgr.wireTopicFailover(...) must be called during bootstrap."
            );
        }
    }

    /**
     * Picks the region currently in {@code Producing} state (skipping {@code targetRegion}).
     * Falls back to "any non-target region" if no clear "Producing" winner exists — the
     * executor's SWITCH stage will surface a clearer error in that pathological case.
     */
    private String resolveActiveRegion(VaradhiTopic topic, String targetRegion) {
        return topic.getInternalTopics().entrySet().stream()
                    .filter(e -> !e.getKey().equals(targetRegion))
                    .filter(e -> e.getValue() != null && e.getValue().getTopicState() != null
                                 && e.getValue().getTopicState().isProduceAllowed())
                    .map(java.util.Map.Entry::getKey)
                    .findFirst()
                    .orElseGet(() -> topic.getInternalTopics().keySet().stream()
                                          .filter(r -> !r.equals(targetRegion))
                                          .findFirst()
                                          .orElseThrow(() -> new InvalidOperationForResourceException(
                                              "Topic " + topic.getName() + " has no source region distinct from "
                                              + targetRegion
                                          )));
    }

    private OpExecutor<OrderedOperation> getOpExecutor(
        SubscriptionOperation operation,
        VaradhiSubscription subscription
    ) {
        //TODO::Better handling needed
        if (operation.getData() instanceof SubscriptionOperation.StartData) {
            return new StartOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                subscriptionStore
            );
        } else if (operation.getData() instanceof SubscriptionOperation.StopData) {
            return new StopOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                subscriptionStore
            );
        } else if (operation.getData() instanceof SubscriptionOperation.ReassignShardData) {
            return new ReAssignOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                subscriptionStore
            );
        } else if (operation.getData() instanceof SubscriptionOperation.UnsidelineData) {
            return new UnsidelinepOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                subscriptionStore
            );
        } else {
            throw new IllegalArgumentException("Can't get OpExecutor for Operation %s.".formatted(operation.getData()));
        }
    }
}

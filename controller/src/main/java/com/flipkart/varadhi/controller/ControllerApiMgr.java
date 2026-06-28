package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.ReAssignOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StartOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StopOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.UnsidelinepOpExecutor;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerApi;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.controller.ControllerConsumerApi;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.ConsumerInfo;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.AssignmentState;
import com.flipkart.varadhi.entities.cluster.ConsumerState;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.common.Constants.SYSTEM_IDENTITY;

@Slf4j
public class ControllerApiMgr implements ControllerApi, ControllerConsumerApi {
    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final SubscriptionStore subscriptionStore;
    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final RegionStore regionStore;
    private final VaradhiClusterManager clusterManager;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final TopicFailoverConfig failoverConfig;

    public ControllerApiMgr(
        OperationMgr operationMgr,
        AssignmentManager assignmentManager,
        SubscriptionStore subscriptionStore,
        ConsumerClientFactory consumerClientFactory,
        TransitionStore transitionStore,
        TopicStore topicStore,
        RegionStore regionStore,
        VaradhiClusterManager clusterManager,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        TopicFailoverConfig failoverConfig
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.subscriptionStore = subscriptionStore;
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.regionStore = regionStore;
        this.clusterManager = clusterManager;
        this.messageExchange = messageExchange;
        this.stageAwaiter = stageAwaiter;
        this.failoverConfig = failoverConfig;
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

    @Override
    public CompletableFuture<TopicFailoverOperation> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request
    ) {
        return CompletableFuture.supplyAsync(() -> {
            VaradhiTopic topic = topicStore.get(topicFqn); // throws ResourceNotFoundException if missing
            validateFailoverRegions(topic, request);
            if (transitionStore.exists(topicFqn)) {
                throw new InvalidOperationForResourceException(
                    "An active failover already exists for topic " + topicFqn + "."
                );
            }
            TopicFailoverOperation op = TopicFailoverOperation.of(
                topicFqn,
                request.sourceRegion(),
                request.targetRegion(),
                request.waitForReplicationLagToClear(),
                request.requestedBy()
            );
            // Atomic create is the lock-free uniqueness guard; a concurrent request fails here.
            transitionStore.create(
                TransitionObject.forFailover(op.getId(), topicFqn, request.sourceRegion(), request.targetRegion())
            );
            log.info(
                "Created topic failover op {} for {} ({}->{})",
                op.getId(),
                topicFqn,
                request.sourceRegion(),
                request.targetRegion()
            );
            operationMgr.createAndEnqueueTopicFailover(op, newFailoverExecutor());
            return op;
        });
    }

    @Override
    public CompletableFuture<TransitionObject> getTopicFailover(String topicFqn) {
        return CompletableFuture.supplyAsync(() -> {
            if (!transitionStore.exists(topicFqn)) {
                throw new ResourceNotFoundException("No active failover for topic " + topicFqn + ".");
            }
            return transitionStore.get(topicFqn);
        });
    }

    @Override
    public CompletableFuture<TransitionObject> abortTopicFailover(String topicFqn, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> {
            if (!transitionStore.exists(topicFqn)) {
                throw new ResourceNotFoundException("No active failover for topic " + topicFqn + ".");
            }
            TransitionObject transition = transitionStore.get(topicFqn);
            if (!transition.isAbortable()) {
                throw new InvalidOperationForResourceException(
                    "Failover for " + topicFqn + " is not abortable in stage " + transition.getCurrentStage() + "."
                );
            }
            log.info(
                "Aborting topic failover op {} for {} (requestedBy={})",
                transition.getOperationId(),
                topicFqn,
                requestedBy
            );
            // Notify pods best-effort, fail the in-flight stage barrier (which fails the executor and
            // marks the op ERRORED), then drop the master so retries/resume become no-ops.
            broadcastTransition(
                TransitionEvent.of(
                    transition.getOperationId(),
                    VaradhiTopicName.parse(topicFqn),
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.ABORTED,
                    0L,
                    null
                )
            );
            stageAwaiter.abort(transition.getOperationId(), "aborted by " + requestedBy);
            transition.advanceTo(TransitionStage.ABORTED, 0L);
            transitionStore.delete(topicFqn);
            return transition;
        });
    }

    @Override
    public CompletableFuture<List<TransitionObject>> getActiveFailovers() {
        return CompletableFuture.supplyAsync(transitionStore::listActive);
    }

    /** Routes a pod ack to the matching stage barrier. Invoked from the controller ack send-handler. */
    public void recordFailoverAck(TransitionAck ack) {
        log.debug("Failover ack op={} host={} stage={} ok={}", ack.opId(), ack.hostname(), ack.stage(), ack.success());
        stageAwaiter.recordAck(ack);
    }

    public List<TopicFailoverOperation> getPendingTopicFailoverOps() {
        return operationMgr.getPendingTopicFailoverOps();
    }

    public void retryTopicFailover(TopicFailoverOperation operation) {
        operationMgr.enqueueTopicFailover(operation, newFailoverExecutor());
    }

    private TopicFailoverOpExecutor newFailoverExecutor() {
        return new TopicFailoverOpExecutor(
            operationMgr,
            transitionStore,
            topicStore,
            messageExchange,
            stageAwaiter,
            clusterManager,
            failoverConfig
        );
    }

    private void broadcastTransition(TransitionEvent event) {
        messageExchange.publish(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.STAGE_BROADCAST_API,
            ClusterMessage.of(event)
        );
    }

    private void validateFailoverRegions(VaradhiTopic topic, TopicFailoverRequest request) {
        RegionName source = request.sourceRegion();
        RegionName target = request.targetRegion();
        if (source == null || target == null) {
            throw new IllegalArgumentException("sourceRegion and targetRegion are required for failover.");
        }
        if (source.equals(target)) {
            throw new IllegalArgumentException("sourceRegion and targetRegion must differ.");
        }
        requireRegisteredRegion(source);
        requireRegisteredRegion(target);
        if (topic.getProduceTopicForRegion(source.value()) == null) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for sourceRegion " + source.value() + "."
            );
        }
        if (topic.getProduceTopicForRegion(target.value()) == null) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for targetRegion " + target.value() + "."
            );
        }
        RegionName active = topic.getActiveRegion();
        if (active != null) {
            if (!source.equals(active)) {
                throw new IllegalArgumentException(
                    "sourceRegion must match the topic activeRegion (" + active.value() + ")."
                );
            }
            if (target.equals(active)) {
                throw new IllegalArgumentException("targetRegion is already the active produce region.");
            }
        }
    }

    private void requireRegisteredRegion(RegionName region) {
        if (!regionStore.exists(region.value())) {
            throw new IllegalArgumentException("Region " + region.value() + " is not registered.");
        }
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

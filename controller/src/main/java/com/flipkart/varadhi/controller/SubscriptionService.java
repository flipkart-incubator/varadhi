package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.ReAssignOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StartOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.StopOpExecutor;
import com.flipkart.varadhi.controller.impl.opexecutors.UnsidelinepOpExecutor;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.ConsumerInfo;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerApi;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.controller.ConsumerCallbackApi;
import com.flipkart.varadhi.core.cluster.controller.SubscriptionApi;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.TopicProduceConfigs;
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
import com.flipkart.varadhi.entities.cluster.failover.*;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.common.Constants.SYSTEM_IDENTITY;

/**
 * Controller-side subscription lifecycle + consumer membership / shard-op callbacks.
 */
@Slf4j
public class SubscriptionService implements SubscriptionApi, ConsumerCallbackApi {

    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final SubscriptionStore subscriptionStore;
    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final RegionStore regionStore;
    private final StorageTopicService storageTopicService;
    private final VaradhiClusterManager clusterManager;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final TopicFailoverConfig failoverConfig;
    private final RegionName deployedRegion;

    public SubscriptionService(
        OperationMgr operationMgr,
        AssignmentManager assignmentManager,
        SubscriptionStore subscriptionStore,
        ConsumerClientFactory consumerClientFactory,
        TransitionStore transitionStore,
        TopicStore topicStore,
        RegionStore regionStore,
        StorageTopicService storageTopicService,
        VaradhiClusterManager clusterManager,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        TopicFailoverConfig failoverConfig,
        RegionName deployedRegion
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.subscriptionStore = subscriptionStore;
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.regionStore = regionStore;
        this.storageTopicService = storageTopicService;
        this.clusterManager = clusterManager;
        this.messageExchange = messageExchange;
        this.stageAwaiter = stageAwaiter;
        this.failoverConfig = failoverConfig;
        this.deployedRegion = deployedRegion;
        this.operationMgr.setTopicFailoverTerminalFailureHandler(this::cleanupFailedTopicFailover);
    }

    private void cleanupFailedTopicFailover(TopicFailoverOperation op) {
        String topicFqn = op.getTopicFqn();
        if (!transitionStore.exists(topicFqn)) {
            return;
        }
        TransitionMaster transition = transitionStore.get(topicFqn);
        log.warn("Cleaning up failed topic failover op {} for {} (error={})", op.getId(), topicFqn, op.getErrorMsg());
        broadcastTransition(
            TransitionEvent.of(
                transition.getOperationId(),
                VaradhiTopicName.parse(topicFqn),
                TransitionType.TOPIC_FAILOVER,
                TransitionStage.ABORTED,
                false,
                0L,
                null
            )
        );
        stageAwaiter.abort(transition.getOperationId(), op.getErrorMsg());
        transitionStore.delete(topicFqn);
    }

    @Override
    public CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> subscriptionStore.get(subscriptionId))
                                .thenCompose(this::getSubscriptionState);
    }

    CompletableFuture<SubscriptionState> getSubscriptionState(VaradhiSubscription subscription) {
        String subId = subscription.getName();
        return CompletableFuture.supplyAsync(() -> assignmentManager.getSubAssignments(subId))
                                .thenCompose(
                                    assignments -> getSubscriptionShardsState(subscription, assignments, subId)
                                )
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

    public CompletableFuture<TopicFailoverOperation> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
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
                requestedBy
            );
            // Atomic create is the lock-free uniqueness guard; a concurrent request fails here.
            transitionStore.create(
                TransitionMaster.forFailover(op.getId(), topicFqn, request.sourceRegion(), request.targetRegion())
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

    public CompletableFuture<TopicFailoverOperation> getTopicFailover(String topicFqn) {
        return CompletableFuture.supplyAsync(() -> {
            if (!transitionStore.exists(topicFqn)) {
                throw new ResourceNotFoundException("No active failover for topic " + topicFqn + ".");
            }
            TransitionMaster transition = transitionStore.get(topicFqn);
            return operationMgr.getTopicFailoverOp(transition.getOperationId());
        });
    }

    public CompletableFuture<TopicFailoverOperation> abortTopicFailover(String topicFqn, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> {
            if (!transitionStore.exists(topicFqn)) {
                throw new ResourceNotFoundException("No active failover for topic " + topicFqn + ".");
            }
            TransitionMaster transition = transitionStore.get(topicFqn);
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
                    false,
                    0L,
                    null
                )
            );
            stageAwaiter.abort(transition.getOperationId(), "aborted by " + requestedBy);
            TopicFailoverOperation failoverOp = operationMgr.getTopicFailoverOp(transition.getOperationId());
            failoverOp.beginStage(TransitionStage.ABORTED);
            operationMgr.updateTopicFailoverOp(failoverOp);
            transition.advanceTo(TransitionStage.ABORTED, 0L);
            transitionStore.delete(topicFqn);
            return failoverOp;
        });
    }

    public CompletableFuture<List<TransitionMaster>> getActiveFailovers() {
        return CompletableFuture.supplyAsync(transitionStore::listActive);
    }

    /** Routes a pod ack to the matching stage barrier. Invoked from the controller ack send-handler. */
    public void recordFailoverAck(TransitionAck ack) {
        log.debug(
            "Failover ack op={} host={} stage={} ok={}",
            ack.opId(),
            ack.hostname(),
            ack.stage(),
            ack.isSuccess()
        );
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
            storageTopicService,
            messageExchange,
            stageAwaiter,
            clusterManager,
            failoverConfig,
            deployedRegion
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
        if (topic.getProduceConfig(source).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for sourceRegion " + source.value() + "."
            );
        }
        if (topic.getProduceConfig(target).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for targetRegion " + target.value() + "."
            );
        }
        if (topic.getStorageTopic() == null) {
            throw new IllegalArgumentException("Topic " + topic.getName() + " has no storage topic.");
        }
        RegionName producing = TopicProduceConfigs.findProducingRegion(topic, deployedRegion).orElse(null);
        if (producing != null) {
            if (!source.equals(producing)) {
                throw new IllegalArgumentException(
                    "sourceRegion must match the topic producing region (" + producing.value() + ")."
                );
            }
            if (target.equals(producing)) {
                throw new IllegalArgumentException("targetRegion is already the producing region.");
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

package com.flipkart.varadhi.controller;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.controller.impl.opexecutors.*;
import com.flipkart.varadhi.core.cluster.entities.*;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.SYSTEM_IDENTITY;

@Slf4j
public class ControllerApiMgr implements ControllerRestApi, ControllerConsumerApi {
    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final MetaStore metaStore;
    private final OperationMgr operationMgr;

    public ControllerApiMgr(
        OperationMgr operationMgr,
        AssignmentManager assignmentManager,
        MetaStore metaStore,
        ConsumerClientFactory consumerClientFactory
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.metaStore = metaStore;
        this.operationMgr = operationMgr;
    }

    @Override
    public CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
                                .thenCompose(this::getSubscriptionState);
    }

    CompletableFuture<SubscriptionState> getSubscriptionState(VaradhiSubscription subscription) {
        String subId = subscription.getName();
        return CompletableFuture.supplyAsync(() -> assignmentManager.getSubAssignments(subId))
                                .thenCompose(assignments -> {
                                    List<CompletableFuture<Optional<ConsumerState>>> shardFutures = assignments.stream()
                                                                                                               .map(
                                                                                                                   a -> {
                                                                                                                       ConsumerApi consumer =
                                                                                                                           consumerClientFactory.getInstance(
                                                                                                                               a.getConsumerId()
                                                                                                                           );
                                                                                                                       return consumer.getConsumerState(
                                                                                                                           subId,
                                                                                                                           a.getShardId()
                                                                                                                       )
                                                                                                                                      .handle(
                                                                                                                                          (
                                                                                                                                              state,
                                                                                                                                              t
                                                                                                                                          ) -> {
                                                                                                                                              if (t
                                                                                                                                                  != null) {
                                                                                                                                                  return Optional.<ConsumerState>empty();
                                                                                                                                              }
                                                                                                                                              return state;
                                                                                                                                          }
                                                                                                                                      );
                                                                                                                   }
                                                                                                               )
                                                                                                               .toList();

                                    return CompletableFuture.allOf(shardFutures.toArray(CompletableFuture[]::new))
                                                            .thenApply(v -> {
                                                                List<Optional<ConsumerState>> states =
                                                                    new ArrayList<>();
                                                                shardFutures.forEach(sf -> states.add(sf.join()));
                                                                return getSubscriptionStatusFromShardStatus(
                                                                    subscription,
                                                                    assignments,
                                                                    states
                                                                );
                                                            });
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
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
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
                                            metaStore
                                        )
                                    );
                                    return operation;
                                }));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
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
                                            metaStore
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
        return CompletableFuture.supplyAsync(() -> metaStore.getSubscription(subscriptionId))
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
                                            metaStore
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
                VaradhiSubscription subscription = metaStore.getSubscription(assignment.getSubscriptionId());
                operationMgr.createAndEnqueue(
                    operation,
                    new ReAssignOpExecutor(
                        subscription,
                        consumerClientFactory,
                        operationMgr,
                        assignmentManager,
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
        SubscriptionOperation operation,
        VaradhiSubscription subscription
    ) {
        //TODO::Better handling needed
        if (operation.getData() instanceof SubscriptionOperation.StartData) {
            return new StartOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else if (operation.getData() instanceof SubscriptionOperation.StopData) {
            return new StopOpExecutor(subscription, consumerClientFactory, operationMgr, assignmentManager, metaStore);
        } else if (operation.getData() instanceof SubscriptionOperation.ReassignShardData) {
            return new ReAssignOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                metaStore
            );
        } else if (operation.getData() instanceof SubscriptionOperation.UnsidelineData) {
            return new UnsidelinepOpExecutor(
                subscription,
                consumerClientFactory,
                operationMgr,
                assignmentManager,
                metaStore
            );
        } else {
            throw new IllegalArgumentException("Can't get OpExecutor for Operation %s.".formatted(operation.getData()));
        }
    }
}

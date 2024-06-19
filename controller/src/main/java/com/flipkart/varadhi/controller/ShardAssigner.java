package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.ConsumerNode;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class ShardAssigner {
    private final String metricPrefix = "controller.assigner";
    private final AssignmentStrategy strategy;
    private final Map<String, ConsumerNode> consumerNodes;
    private final AssignmentStore assignmentStore;
    private final ExecutorService executor;

    public ShardAssigner(AssignmentStore assignmentStore, MeterRegistry meterRegistry) {
        this.strategy = new LeastAssignedStrategy();
        this.consumerNodes = new ConcurrentHashMap<>();
        this.assignmentStore = assignmentStore;
        //TODO::ExecutorService should emit the metrics.
        this.executor =
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("assigner-%d").build());
    }

    public void addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        clusterConsumers.forEach(c -> {
            addConsumerNode(c);
            log.info("Added consumer node {}", c.getConsumerId());
        });
    }

    // idempotency -- does not re-assign if shard is already assigned, however they are returned.
    public CompletableFuture<List<Assignment>> assignShard(
            List<SubscriptionUnitShard> shards, VaradhiSubscription subscription, Set<String> nodesToExclude
    ) {
        //TODO:: do not assign the > 1 shard of same sub to same consumer node.
        return CompletableFuture.supplyAsync(() -> {
            List<ConsumerNode> activeConsumers =
                    consumerNodes.values().stream()
                            .filter(c -> !c.isMarkedForDeletion() && !nodesToExclude.contains(c.getConsumerId()))
                            .collect(Collectors.toList());
            log.info(
                    "AssignShards found consumer nodes active:{} of total:{}", activeConsumers.size(),
                    consumerNodes.size()
            );

            List<Assignment> assignments = new ArrayList<>();
            try {

                List<SubscriptionUnitShard> unAssignedShards = new ArrayList<>();
                List<Assignment> alreadyAssigned = new ArrayList<>();

                Map<Integer, Assignment> existingAssignments =
                        assignmentStore.getSubscriptionAssignments(subscription.getName()).stream().collect(
                                Collectors.toMap(Assignment::getShardId, a -> a));

                // create new assignments only for shards which are still un-assigned.
                shards.forEach(s -> {
                    if (existingAssignments.containsKey(s.getShardId())) {
                        alreadyAssigned.add(existingAssignments.get(s.getShardId()));
                    } else {
                        unAssignedShards.add(s);
                    }
                });

                alreadyAssigned.forEach(
                        a -> log.info("Assignment {} already exists for shard {}, skipping assignment.", a,
                                a.getName()
                        ));

                assignments.addAll(strategy.assign(unAssignedShards, subscription, activeConsumers));
                log.info(
                        "Assign Requested:{}, Already Assigned:{}, Newly Assigned:{}.", shards.size(),
                        alreadyAssigned.size(), assignments.size()
                );
                assignmentStore.createAssignments(assignments);
                // assignments which are already done are returned as well.
                assignments.addAll(alreadyAssigned);
                return assignments;
            } catch (Exception e) {
                log.error("Failed while creating assignment, freeing up any allocation done. {}.", e.getMessage());
                SubscriptionShards subShards = subscription.getShards();
                assignments.forEach(
                        assignment -> freeCapacityFromNode(assignment, subShards.getShard(assignment.getShardId())));
                throw e;
            }
        }, executor);
    }


    // idempotency -- should not fail, if already un-assigned.
    public CompletableFuture<Void> unAssignShard(
            List<Assignment> assignments, VaradhiSubscription subscription, boolean freeAssignedCapacity
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<Assignment> toDelete = new ArrayList<>();
                List<Assignment> alreadyDeleted = new ArrayList<>();

                Set<Assignment> existingAssignments =
                        new HashSet<>(assignmentStore.getSubscriptionAssignments(subscription.getName()));

                // delete  assignments only for shards which are still assigned.
                assignments.forEach(a -> {
                    if (existingAssignments.contains(a)) {
                        toDelete.add(a);
                    } else {
                        alreadyDeleted.add(a);
                    }
                });

                alreadyDeleted.forEach(
                        a -> log.info("Assignment {} already deleted for shard {}, skipping delete.", a, a.getName()));

                assignmentStore.deleteAssignments(toDelete);

                log.info(
                        "UnAssign Requested:{}, Already UnAssigned:{}, Newly UnAssigned:{}.", assignments.size(),
                        alreadyDeleted.size(), toDelete.size()
                );

                if (freeAssignedCapacity) {
                    SubscriptionShards shards = subscription.getShards();
                    assignments.forEach(a -> freeCapacityFromNode(a, shards.getShard(a.getShardId())));
                }
                return null;
            } catch (Exception e) {
                log.error("Failed while unAssigning Shards. {}.", e.getMessage());
                throw e;
            }
        }, executor);
    }

    public CompletableFuture<Assignment> reAssignShard(
            Assignment assignment, VaradhiSubscription subscription, boolean freeAssignedCapacity
    ) {
        Set<String> nodeToExclude = new HashSet<>();
        ConsumerNode assignedNode = consumerNodes.getOrDefault(assignment.getConsumerId(), null);
        if (null != assignedNode) {
            nodeToExclude.add(assignedNode.getConsumerId());
        }
        unAssignShard(List.of(assignment), subscription, freeAssignedCapacity);
        List<SubscriptionUnitShard> shardToReAssign =
                List.of(subscription.getShards().getShard(assignment.getShardId()));
        return assignShard(shardToReAssign, subscription, nodeToExclude).thenApply(assignments -> assignments.get(0));
    }

    private void freeCapacityFromNode(Assignment assignment, SubscriptionUnitShard shard) {
        String consumerId = assignment.getConsumerId();
        ConsumerNode consumerNode = consumerNodes.getOrDefault(consumerId, null);
        if (null == consumerNode) {
            log.error("Consumer node not found, for assignment {}. Ignoring unAssignShard", assignment);
        } else {
            consumerNode.free(assignment, shard.getCapacityRequest());
        }
    }

    public List<Assignment> getSubscriptionAssignment(String subscriptionName) {
        return assignmentStore.getSubscriptionAssignments(subscriptionName);
    }

    public List<Assignment> getConsumerNodeAssignment(String consumerNodeId) {
        return assignmentStore.getConsumerNodeAssignments(consumerNodeId);
    }

    public void consumerNodeJoined(ConsumerNode consumerNode) {
        boolean added = addConsumerNode(consumerNode);
        if (added) {
            log.info("ConsumerNode {} joined.", consumerNode.getConsumerId());
        }
    }

    public void consumerNodeLeft(String consumerNodeId) {
        //TODO:: re-assign the shards (should this be trigger from here or from the controller) ?
        MutableBoolean marked = new MutableBoolean(false);
        consumerNodes.computeIfPresent(consumerNodeId, (k, v) -> {
            v.markForDeletion();
            marked.setTrue();
            return v;
        });
        if (marked.booleanValue()) {
            log.info("ConsumerNode {} marked for deletion.", consumerNodeId);
        } else {
            log.warn("ConsumerNode {} not found.", consumerNodeId);
        }
    }

    private boolean addConsumerNode(ConsumerNode consumerNode) {
        String consumerNodeId = consumerNode.getConsumerId();
        MutableBoolean added = new MutableBoolean(false);
        consumerNodes.computeIfAbsent(consumerNodeId, k -> {
            added.setTrue();
            return consumerNode;
        });
        if (!added.booleanValue()) {
            log.warn("ConsumerNode {} already exists. Not adding again.", consumerNodes.get(consumerNodeId));
        }
        return added.booleanValue();
    }

}

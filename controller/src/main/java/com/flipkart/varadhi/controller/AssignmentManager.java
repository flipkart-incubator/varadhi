package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.core.cluster.entities.ConsumerNode;
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
public class AssignmentManager {
    private final String metricPrefix = "controller.assignmentMgr";
    private final AssignmentStrategy strategy;
    private final Map<String, ConsumerNode> consumerNodes;
    private final AssignmentStore assignmentStore;
    private final ExecutorService executor;

    public AssignmentManager(
        AssignmentStrategy strategy,
        AssignmentStore assignmentStore,
        MeterRegistry meterRegistry
    ) {
        this.strategy = strategy;
        this.consumerNodes = new ConcurrentHashMap<>();
        this.assignmentStore = assignmentStore;
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("assigner-%d").build()
        );
    }


    // idempotency -- does not re-assign if shard is already assigned, however they are returned.
    public CompletableFuture<List<Assignment>> assignShards(
        List<SubscriptionUnitShard> shards,
        VaradhiSubscription subscription,
        List<String> nodesToExclude
    ) {
        return CompletableFuture.supplyAsync(() -> {
            List<Assignment> newAssignments = new ArrayList<>();
            try {
                Map<Integer, Assignment> alreadyAssigned = getExistingAssignments(shards, subscription);
                log.info("Found {} assignments for {}.", alreadyAssigned.size(), subscription.getName());

                List<SubscriptionUnitShard> unAssignedShards = shards.stream()
                                                                     .filter(
                                                                         s -> !alreadyAssigned.containsKey(
                                                                             s.getShardId()
                                                                         )
                                                                     )
                                                                     .collect(Collectors.toList());
                // create new assignments only for shards which are still un-assigned.
                newAssignments.addAll(doAssignments(unAssignedShards, subscription, nodesToExclude));
                assignmentStore.createAssignments(newAssignments);
                log.info("Created {} new Assignments for {}.", newAssignments.size(), subscription.getName());

                // assignments which were already assigned are returned as well.
                List<Assignment> allAssignments = new ArrayList<>(alreadyAssigned.values());
                allAssignments.addAll(newAssignments);
                return allAssignments;
            } catch (Exception e) {
                log.error("Failed while creating assignment, freeing up any allocation done. {}.", e.getMessage());
                SubscriptionShards subShards = subscription.getShards();
                newAssignments.forEach(
                    assignment -> freeCapacityFromNode(assignment, subShards.getShard(assignment.getShardId()))
                );
                throw e;
            }
        }, executor);
    }

    private Map<Integer, Assignment> getExistingAssignments(
        List<SubscriptionUnitShard> shards,
        VaradhiSubscription subscription
    ) {
        Map<Integer, Assignment> assigned = assignmentStore.getSubAssignments(subscription.getName())
                                                           .stream()
                                                           .collect(Collectors.toMap(Assignment::getShardId, a -> a));

        return shards.stream()
                     .filter(s -> assigned.containsKey(s.getShardId()))
                     .collect(Collectors.toMap(SubscriptionUnitShard::getShardId, s -> assigned.get(s.getShardId())));
    }

    private List<Assignment> doAssignments(
        List<SubscriptionUnitShard> unAssignedShards,
        VaradhiSubscription subscription,
        List<String> nodesToExclude
    ) {
        List<ConsumerNode> activeConsumers = getActiveConsumers(nodesToExclude);
        return strategy.assign(unAssignedShards, subscription, activeConsumers);
    }

    private List<ConsumerNode> getActiveConsumers(List<String> nodesToExclude) {
        Set<String> exclusions = new HashSet<>(nodesToExclude);
        List<ConsumerNode> activeConsumers = consumerNodes.values()
                                                          .stream()
                                                          .filter(c -> !exclusions.contains(c.getConsumerId()))
                                                          .collect(Collectors.toList());
        log.info("Found {} active consumer nodes of total {}.", activeConsumers.size(), consumerNodes.size());
        return activeConsumers;
    }


    // idempotency -- should not fail, if already un-assigned.
    public CompletableFuture<Void> unAssignShards(
        List<Assignment> assignments,
        VaradhiSubscription subscription,
        boolean freeAssignedCapacity
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<Assignment> toDelete = new ArrayList<>();
                List<Assignment> alreadyDeleted = new ArrayList<>();

                Set<Assignment> existingAssignments = new HashSet<>(
                    assignmentStore.getSubAssignments(subscription.getName())
                );

                // delete  assignments only for shards which are still assigned.
                assignments.forEach(a -> {
                    if (existingAssignments.contains(a)) {
                        toDelete.add(a);
                    } else {
                        alreadyDeleted.add(a);
                    }
                });

                alreadyDeleted.forEach(
                    a -> log.info("Assignment {} already deleted for shard {}, skipping delete.", a, a.getName())
                );

                assignmentStore.deleteAssignments(toDelete);

                log.info(
                    "UnAssign Requested:{}, Already UnAssigned:{}, Newly UnAssigned:{}.",
                    assignments.size(),
                    alreadyDeleted.size(),
                    toDelete.size()
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
        Assignment assignment,
        VaradhiSubscription subscription,
        boolean freeAssignedCapacity
    ) {
        List<String> nodeToExclude = new ArrayList<>();
        ConsumerNode assignedNode = consumerNodes.getOrDefault(assignment.getConsumerId(), null);
        if (null != assignedNode) {
            nodeToExclude.add(assignedNode.getConsumerId());
        }
        List<SubscriptionUnitShard> shardToReAssign = List.of(
            subscription.getShards().getShard(assignment.getShardId())
        );

        return unAssignShards(List.of(assignment), subscription, freeAssignedCapacity).thenCompose(
            v -> assignShards(shardToReAssign, subscription, nodeToExclude).thenApply(assignments -> assignments.get(0))
        );
    }

    private void freeCapacityFromNode(Assignment assignment, SubscriptionUnitShard shard) {
        String consumerId = assignment.getConsumerId();
        ConsumerNode consumerNode = consumerNodes.getOrDefault(consumerId, null);
        if (null == consumerNode) {
            log.warn("Consumer node not found, for assignment {}. Ignoring free capacity for it.", assignment);
        } else {
            consumerNode.free(assignment, shard.getCapacityRequest());
        }
    }

    public List<Assignment> getSubAssignments(String subscriptionName) {
        return assignmentStore.getSubAssignments(subscriptionName);
    }

    public List<Assignment> getConsumerNodeAssignments(String consumerNodeId) {
        return assignmentStore.getConsumerNodeAssignments(consumerNodeId);
    }

    public List<Assignment> getAllAssignments() {
        return assignmentStore.getAllAssignments();
    }

    public CompletableFuture<Void> consumerNodeJoined(ConsumerNode consumerNode) {
        return CompletableFuture.runAsync(() -> {
            boolean added = addConsumerNode(consumerNode);
            if (added) {
                log.info("ConsumerNode {} joined.", consumerNode.getConsumerId());
            }
        }, executor);
    }

    public CompletableFuture<Void> consumerNodeLeft(String consumerNodeId) {
        return CompletableFuture.runAsync(() -> {
            if (null != consumerNodes.remove(consumerNodeId)) {
                log.info("ConsumerNode {} removed.", consumerNodeId);
            } else {
                log.warn("ConsumerNode {} not found.", consumerNodeId);
            }
        }, executor);
    }

    //TODO::Fix it.. ensure this is used during init only ??
    public boolean addConsumerNode(ConsumerNode consumerNode) {
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

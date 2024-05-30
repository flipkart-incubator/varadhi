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
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("assigner-%d").build());
    }

    public void addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        clusterConsumers.forEach(c -> {
            addConsumerNode(c);
            log.info("Added consumer node {}", c.getMemberInfo().hostname());
        });
    }

    public CompletableFuture<List<Assignment>> assignShard(
            List<SubscriptionUnitShard> shards, VaradhiSubscription subscription
    ) {
        return CompletableFuture.supplyAsync(() -> {
            List<ConsumerNode> activeConsumers =
                    consumerNodes.values().stream().filter(c -> !c.isMarkedForDeletion()).collect(Collectors.toList());
            log.info(
                    "AssignShards found consumer nodes active:{} of total:{}", activeConsumers.size(),
                    consumerNodes.size()
            );

            List<Assignment> assignments = new ArrayList<>();
            try {
                assignments.addAll(strategy.assign(shards, subscription, activeConsumers));
                assignmentStore.createAssignments(assignments);
                return assignments;
            } catch (Exception e) {
                log.error("Failed while creating assignment, freeing up any allocation done. {}.", e.getMessage());
                assignments.forEach(assignment -> freeAssignedCapacity(assignment, subscription));
                throw e;
            }
        }, executor);
    }


    public CompletableFuture<Void> unAssignShard(List<Assignment> assignments, VaradhiSubscription subscription) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                assignmentStore.deleteAssignments(assignments);
                assignments.forEach(a -> {
                    String consumerId = a.getConsumerId();
                    ConsumerNode consumerNode = consumerNodes.getOrDefault(consumerId, null);
                    if (null == consumerNode) {
                        log.error("Consumer node not found, for assignment {}. Ignoring unAssignShard", a);
                    } else {
                        SubscriptionShards shards = subscription.getShards();
                        consumerNode.free(a, shards.getShard(a.getShardId()).getCapacityRequest());
                    }
                });
                return null;
            } catch (Exception e) {
                log.error("Failed while unAssigning Shards. {}.", e.getMessage());
                throw e;
            }
        }, executor);
    }

    private void freeAssignedCapacity(Assignment assignment, VaradhiSubscription subscription) {
        SubscriptionUnitShard shard = subscription.getShards().getShard(assignment.getShardId());
        ConsumerNode consumerNode = consumerNodes.get(assignment.getConsumerId());
        consumerNode.free(assignment, shard.getCapacityRequest());
    }


    public List<Assignment> getSubscriptionAssignment(String subscriptionName) {
        return assignmentStore.getSubscriptionAssignments(subscriptionName);
    }

    public void consumerNodeJoined(ConsumerNode consumerNode) {
        boolean added = addConsumerNode(consumerNode);
        if (added) {
            log.info("ConsumerNode {} joined.", consumerNode.getMemberInfo().hostname());
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
        String consumerNodeId = consumerNode.getMemberInfo().hostname();
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

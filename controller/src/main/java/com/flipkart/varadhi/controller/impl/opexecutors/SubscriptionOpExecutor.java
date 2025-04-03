package com.flipkart.varadhi.controller.impl.opexecutors;

import com.flipkart.varadhi.controller.AssignmentManager;
import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
public abstract class SubscriptionOpExecutor implements OpExecutor<OrderedOperation> {
    final VaradhiSubscription subscription;
    final ConsumerClientFactory consumerClientFactory;
    final OperationMgr operationMgr;
    final AssignmentManager assignmentManager;
    final SubscriptionMetaStore subscriptionMetaStore;

    public abstract CompletableFuture<Void> execute(OrderedOperation operation);

    ConsumerApi getAssignedConsumer(Assignment assignment) {
        return consumerClientFactory.getInstance(assignment.getConsumerId());
    }

    void failShardOperation(ShardOperation shardOp, Throwable t) {
        log.error("shard operation ({}) failed: {}.", shardOp, t.getMessage());
        shardOp.markFail(t.getMessage());
        operationMgr.updateShardOp(shardOp);
    }

    void completeSubOperation(SubscriptionOperation subOp) {
        subOp.markCompleted();
        operationMgr.updateSubOp(subOp);
    }

    void failSubOperation(SubscriptionOperation subOp, Throwable t) {
        log.error("Subscription operation ({}) failed: {}.", subOp, t.getMessage());
        subOp.markFail(t.getMessage());
        operationMgr.updateSubOp(subOp);
    }

    boolean allShardsSkipped(List<CompletableFuture<Boolean>> shardFutures) {
        long notScheduled = shardFutures.stream().filter(f -> {
            try {
                return f.isDone() && !f.get();
            } catch (Exception e) {
                // this is not expected.
                log.error(
                    "Unexpected error while retrieving the shard operation scheduling status {}.",
                    e.getMessage()
                );
                throw new RuntimeException(e);
            }
        }).count();
        log.info("Shards total:{} Shards with operation skipped: {}.", shardFutures.size(), notScheduled);
        return notScheduled == shardFutures.size();
    }
}

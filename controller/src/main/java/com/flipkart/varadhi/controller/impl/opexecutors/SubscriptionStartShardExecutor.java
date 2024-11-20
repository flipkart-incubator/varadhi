package com.flipkart.varadhi.controller.impl.opexecutors;

import com.flipkart.varadhi.controller.AssignmentManager;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class SubscriptionStartShardExecutor extends SubscriptionOpExecutor {

    public SubscriptionStartShardExecutor(
            VaradhiSubscription subscription, ConsumerClientFactory clientFactory, OperationMgr operationMgr,
            AssignmentManager assignmentManager, MetaStore metaStore
    ) {
        super(subscription, clientFactory, operationMgr, assignmentManager, metaStore);
    }
    CompletableFuture<Boolean> startShard(ShardOperation startOp, boolean isRetry, ConsumerApi consumer) {
        String subId = startOp.getOpData().getSubscriptionId();
        int shardId = startOp.getOpData().getShardId();
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            // Start can be executed in stopping subscription as well.
            // in general this shouldn't happen as multiple in-progress operations are not allowed.
            if (shardStatus.canSkipStart()) {
                log.info("Subscription:{} Shard:{} is already started. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(false);
            }
            operationMgr.submitShardOp(startOp, isRetry);
            CompletableFuture<Boolean> startFuture =
                    consumer.start((ShardOperation.StartData) startOp.getOpData()).thenApply(v -> true);
            log.info("Scheduled shard start({}).", startOp);
            return startFuture;
        }).exceptionally(t -> {
            failShardOperation(startOp, t);
            return true;
        });
    }
}

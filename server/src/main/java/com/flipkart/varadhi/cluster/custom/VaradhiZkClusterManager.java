package com.flipkart.varadhi.cluster.custom;

import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.config.ZookeeperConnectConfig;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Customized zkClusterManager that only works with provided zk configuration. The curatorFramework, needs to be
 * configured with namespace for the purpose of cluster nodes.
 * In the future, we will customize this so that curator is not required to be namespace aware, instead this class will
 * handle the namespacing all the paths. This should allow usage of curator client for other purposes other than just cluster
 * management. We also need to remove curator.close() from leave method, in case the curator instance is provided.
 * This class also customizes 1 method:
 * 1. setNodeInfo() - to include {@link MemberInfo} as part of the nodeInfo
 */
@Slf4j
public class VaradhiZkClusterManager extends ZookeeperClusterManager implements VaradhiClusterManager {
    private final DeliveryOptions deliveryOptions;
    private final RetryPolicy<NodeInfo> NodeInfoRetryPolicy = RetryPolicy
            .<NodeInfo>builder()
            .withMaxAttempts(50)
            .withDelay(Duration.ofMillis(200))
            .onRetry(
                    e -> log.warn("Failed to get nodeInfo error:{}. Retrying ... {}", e.getLastException().getMessage(),
                            e.getAttemptCount()
                    ))
            .build();

    public VaradhiZkClusterManager(ZookeeperConnectConfig zkConnectConfig, String host) {
        super(CuratorFrameworkCreator.create(zkConnectConfig), host);
        //TODO:: Add config details to DeliveryOptions e.g. timeouts, tracing etc as part of cluster manager changes.
        deliveryOptions = new DeliveryOptions();
    }

    @Override
    public List<MemberInfo> getAllMembers() {
        throw new NotImplementedException("getAllMembers not implemented");
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {
        nodeListener(new NodeListener() {
            @Override
            public void nodeAdded(String nodeId) {
                Failsafe.with(NodeInfoRetryPolicy).getStageAsync(() -> fetchNodeInfo(nodeId).toCompletionStage())
                        .whenComplete((nodeInfo, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to get nodeInfo for node: {}.", nodeId, throwable);
                            } else {
                                log.debug("Node {} joined from {}:{}.", nodeId, nodeInfo.host(), nodeInfo.port());
                                MemberInfo memberInfo = nodeInfo.metadata().mapTo(MemberInfo.class);
                                listener.joined(memberInfo);
                            }
                        });
            }

            @Override
            public void nodeLeft(String nodeId) {
                log.debug("Node {} left.", nodeId);
                listener.left(nodeId);
            }
        });
    }

    @Override
    public MessageRouter getRouter(Vertx vertx) {
        return new MessageRouter(vertx.eventBus(), deliveryOptions);
    }

    @Override
    public MessageExchange getExchange(Vertx vertx) {
        return new MessageExchange(vertx.eventBus(), deliveryOptions);
    }

    public Future<Void> lock(String lockName) {
        throw new NotImplementedException("lock not implemented");
    }

    private Future<NodeInfo> fetchNodeInfo(String nodeId) {
        Promise<NodeInfo> promise = Promise.promise();
        getNodeInfo(nodeId, promise);
        return promise.future();
    }
}

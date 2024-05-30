package com.flipkart.varadhi.cluster.custom;

import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.entities.cluster.MemberInfo;
import com.flipkart.varadhi.utils.JsonMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Customized zkClusterManager that only works with provided zk configuration. The curatorFramework, needs to be
 * configured with namespace for the purpose of cluster nodes.
 * In the future, we will customize this so that curator is not required to be namespace aware, instead this class will
 * handle the namespacing all the paths. This should allow usage of curator client for other purposes other than just cluster
 * management. We also need to remove curator.close() from leave method, in case the curator instance is provided.
 */
@Slf4j
public class VaradhiZkClusterManager extends ZookeeperClusterManager implements VaradhiClusterManager {
    private final DeliveryOptions deliveryOptions;
    private final RetryPolicy<NodeInfo> NodeInfoRetryPolicy = RetryPolicy
            .<NodeInfo>builder()
            .withMaxAttempts(10)
            .withDelay(Duration.ofMillis(200))
            .onRetry(
                    e -> log.warn("Failed to get nodeInfo error:{}. Retrying ... {}", e.getLastException().getMessage(),
                            e.getAttemptCount()
                    ))
            .build();

    public VaradhiZkClusterManager(
            CuratorFramework curatorFramework, DeliveryOptions deliveryOptions, String host
    ) {
        super(curatorFramework, host);
        this.deliveryOptions = deliveryOptions == null ? new DeliveryOptions().setSendTimeout(1000).setTracingPolicy(
                TracingPolicy.PROPAGATE) : deliveryOptions;
    }

    @Override
    public Future<List<MemberInfo>> getAllMembers() {
        List<CompletableFuture<MemberInfo>> allFutures = new ArrayList<>();
        getNodes().forEach(
                nodeId -> allFutures.add(Failsafe.with(NodeInfoRetryPolicy)
                        .getStageAsync(() -> fetchNodeInfo(nodeId).toCompletionStage())
                        .thenApply(nodeInfo -> JsonMapper.jsonDeserialize(
                                nodeInfo.metadata().toString(),
                                MemberInfo.class
                        ))
                        .whenComplete((nodeInfo, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to get nodeInfo for node: {}.", nodeId, throwable);
                            }
                        })
                ));
        return Future.fromCompletionStage(CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> allFutures.stream().map(CompletableFuture::join).collect(Collectors.toList())));
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {
        nodeListener(new NodeListener() {
            @Override
            public void nodeAdded(String nodeId) {
                log.debug("Node {} joined.", nodeId);
                Failsafe.with(NodeInfoRetryPolicy).getStageAsync(() -> fetchNodeInfo(nodeId).toCompletionStage())
                        .whenComplete((nodeInfo, throwable) -> {
                            if (throwable != null) {
                                // ignore the failure for now. Listener will not be notified of the change.
                                log.error("Failed to get nodeInfo for member: {}.", nodeId, throwable);
                            } else {
                                try {
                                    log.debug("Member {} joined from {}:{}.", nodeId, nodeInfo.host(), nodeInfo.port());
                                    MemberInfo memberInfo = nodeInfo.metadata().mapTo(MemberInfo.class);
                                    listener.joined(memberInfo)
                                            .exceptionally(t -> {
                                                log.error("MembershipListener.joined({}) failed, {}.", nodeId, t.getMessage());
                                                return null;
                                            });
                                } catch (Exception e) {
                                    log.error("MembershipListener.joined({}) failed, {}.", nodeId, e.getMessage());
                                    throw e;
                                }
                            }
                        });
            }

            @Override
            public void nodeLeft(String nodeId) {
                log.debug("Node {} left.", nodeId);
                try {
                    listener.left(nodeId).exceptionally(t -> {
                        log.error("MembershipListener.left({}) failed, {}.", nodeId, t.getMessage());
                        return null;
                    });
                }catch (Exception e) {
                    log.error("MembershipListener.left({}) failed, {}.", nodeId, e.getMessage());
                    throw e;
                }
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

    private Future<NodeInfo> fetchNodeInfo(String nodeId) {
        Promise<NodeInfo> promise = Promise.promise();
        getNodeInfo(nodeId, promise);
        return promise.future();
    }
}

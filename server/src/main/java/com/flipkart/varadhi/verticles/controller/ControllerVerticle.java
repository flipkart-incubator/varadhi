package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.controller.ShardAssigner;
import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.verticles.consumer.ConsumerClientFactoryImpl;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final MetaStoreProvider metaStoreProvider;
    private final MeterRegistry meterRegistry;
    private final ControllerConfig controllerConfig;

    public ControllerVerticle(
            ControllerConfig config, CoreServices coreServices, VaradhiClusterManager clusterManager
    ) {
        this.controllerConfig = config;
        this.clusterManager = clusterManager;
        this.metaStoreProvider = coreServices.getMetaStoreProvider();
        this.meterRegistry = coreServices.getMeterRegistry();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ControllerApiMgr controllerApiMgr = getControllerApiMgr(messageExchange);
        ControllerApiHandler handler = new ControllerApiHandler(controllerApiMgr);

        //TODO::Assuming one controller node for time being. Leader election needs to be added.
        onLeaderElected(controllerApiMgr, handler, messageRouter).onComplete(ar -> {
            if (ar.failed()) {
                startPromise.fail(ar.cause());
            } else {
                startPromise.complete();
            }
        });
    }

    private ControllerApiMgr getControllerApiMgr(MessageExchange messageExchange) {
        ConsumerClientFactory consumerClientFactory = new ConsumerClientFactoryImpl(messageExchange);
        OperationMgr operationMgr = new OperationMgr(controllerConfig, metaStoreProvider.getOpStore());
        ShardAssigner assigner =
                new ShardAssigner(new LeastAssignedStrategy(), metaStoreProvider.getAssignmentStore(), meterRegistry);
        ControllerApiMgr controllerApiMgr =
                new ControllerApiMgr(operationMgr, assigner, metaStoreProvider.getMetaStore(), consumerClientFactory);
        return controllerApiMgr;
    }

    private Future<Void> onLeaderElected(
            ControllerApiMgr controllerApiMgr, ControllerApiHandler handler, MessageRouter messageRouter
    ) {
        // any failures should give up the leadership.
        //TODO:: check what happens when membership changes post listener setup but prior to bootstrap completion.
        setupMembershipListener(controllerApiMgr);

        return clusterManager.getAllMembers().compose(allMembers -> {
            List<ConsumerNode> consumerNodes =
                    allMembers.stream().filter(memberInfo -> memberInfo.hasRole(ComponentKind.Consumer))
                            .map(ConsumerNode::new).toList();
            log.info("Available Consumer Nodes {}", consumerNodes.size());
            CompletableFuture<String>[] nodeFutures =
                    consumerNodes.stream().map(controllerApiMgr::addConsumerNode).toArray(CompletableFuture[]::new);

            List<String> addedConsumers = new ArrayList<>();
            CompletableFuture<Void> future = CompletableFuture.allOf(nodeFutures)
                    .whenComplete((v, t) -> Arrays.stream(nodeFutures).forEach(nodeFuture -> {
                        try {
                            String consumerId = nodeFuture.join();
                            addedConsumers.add(consumerId);
                        } catch (CompletionException ce) {
                            log.error("ConsumerNode:{} failed to join the cluster {}.", "", ce.getCause().getMessage());
                        }
                    })).thenAccept(v -> {
                        setupApiHandlers(messageRouter, handler);
                        restoreController(controllerApiMgr, addedConsumers);
                    });
            return Future.fromCompletionStage(future);
        }).onComplete(ar -> {
            if (ar.failed()) {
                log.error("Failed to Start controller. Giving up leadership.", ar.cause());
                abortLeaderShip();
            } else {
                log.info("Leadership obtained successfully");
            }
        }).mapEmpty();
    }

    private void restoreController(ControllerApiMgr controllerApiMgr, List<String> consumerIds) {
        // get all subscription assignments
        // find consumer nodes which needs to deleted
        //     for deleted consumer nodes, call consumer node left.
        // all subscription should be usable now, though some may have partial state.
        removeUnavailableConsumers(controllerApiMgr, consumerIds);

        // get all in progress subscription operations.
        // -- requeue the operation again (operation being idempotent, this should work)
        requeueInProgressOperation(controllerApiMgr);
        //TODO:: add handling for failed operations as well (similar to recovery of failure w/o controller restart)
    }

    private void removeUnavailableConsumers(ControllerApiMgr controllerApiMgr, List<String> consumerIds) {
        getUnavailableConsumers(controllerApiMgr, consumerIds).forEach(consumerId -> {
            log.info("Marking consumer {} as left.", consumerId);
            controllerApiMgr.consumerNodeLeft(consumerId);
        });
    }

    private List<String> getUnavailableConsumers(ControllerApiMgr controllerApiMgr, List<String> consumerIds) {
        List<Assignment> allAssignments = controllerApiMgr.getAllAssignments();
        log.info("Found {} assignments", allAssignments.size());
        List<Assignment> zombieAssignments =
                allAssignments.stream().filter(a -> !consumerIds.contains(a.getConsumerId())).toList();
        log.info("Found {} assignments without consumers.", zombieAssignments.size());
        List<String> unavailableConsumers =
                zombieAssignments.stream().map(Assignment::getConsumerId).distinct().collect(Collectors.toList());
        log.info("Found {} unavailable Consumers.", unavailableConsumers.size());
        return unavailableConsumers;
    }

    private void requeueInProgressOperation(ControllerApiMgr controllerApiMgr) {
        List<SubscriptionOperation> pendingSubOps = getPendingSubOps(controllerApiMgr);
        log.info("Found {} inProgress operations.", pendingSubOps.size());
        pendingSubOps.forEach(subOp -> {
            controllerApiMgr.retryOperation(subOp);
            log.info("Requeued pending operation: {}.", subOp);
        });
    }

    private List<SubscriptionOperation> getPendingSubOps(ControllerApiMgr controllerApiMgr) {
        return controllerApiMgr.getPendingSubOps();
    }


    private void abortLeaderShip() {
        throw new NotImplementedException("abortLeaderShip to be implemented.");
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ControllerApiHandler handler) {
        messageRouter.requestHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "stop", handler::stop);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "status", handler::status);
        messageRouter.sendHandler(ROUTE_CONTROLLER, "update", handler::update);
    }

    private void setupMembershipListener(ControllerApiMgr controllerApiMgr) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(MemberInfo memberInfo) {
                log.info("Member joined: {}", memberInfo);
                if (memberInfo.hasRole(ComponentKind.Consumer)) {
                    ConsumerNode consumerNode = new ConsumerNode(memberInfo);
                    return controllerApiMgr.consumerNodeJoined(consumerNode);
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String memberId) {
                // Vertx cluster manager provides only the path (i.e.) id and not data of the node.
                // It will be good, to have a data, so filtering can be done for non-consumer nodes.
                // Currently, this filtering is taken care implicitly.
                log.info("Member left: {}", memberId);
                return controllerApiMgr.consumerNodeLeft(memberId);
            }
        });
    }
}

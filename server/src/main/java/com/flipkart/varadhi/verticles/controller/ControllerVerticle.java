package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.cluster.ConsumerNode;
import com.flipkart.varadhi.entities.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.verticles.consumer.ConsumerClientFactoryImpl;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.entities.cluster.MemberInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final MetaStoreProvider metaStoreProvider;
    private final MeterRegistry meterRegistry;
    private final ControllerConfig controllerConfig;

    public ControllerVerticle(ControllerConfig config, CoreServices coreServices, VaradhiClusterManager clusterManager) {
        this.controllerConfig = config;
        this.clusterManager = clusterManager;
        this.metaStoreProvider = coreServices.getMetaStoreProvider();
        this.meterRegistry = coreServices.getMeterRegistry();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ConsumerClientFactory consumerClientFactory = new ConsumerClientFactoryImpl(messageExchange);

        ControllerApiMgr controllerApiMgr =
                new ControllerApiMgr(controllerConfig, consumerClientFactory, metaStoreProvider, meterRegistry);
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

    private Future<Void> onLeaderElected(
            ControllerApiMgr controllerApiMgr, ControllerApiHandler handler, MessageRouter messageRouter
    ) {
        // any failures should give up the leadership.
        setupMembershipListener(controllerApiMgr);

        return clusterManager.getAllMembers().compose(allMembers -> {
            List<ConsumerNode> consumerNodes =
                    allMembers.stream().filter(memberInfo -> memberInfo.hasRole(ComponentKind.Consumer))
                            .map(ConsumerNode::new).toList();
            log.info("Available Consumer Nodes {}", consumerNodes.size());
            return Future.fromCompletionStage(controllerApiMgr.addConsumerNodes(consumerNodes)
                    .thenAccept(v -> setupApiHandlers(messageRouter, handler)));
        }).onComplete(ar -> {
            if (ar.failed()) {
                log.error("Failed to get all members. Giving up leadership.", ar.cause());
                abortLeaderShip();
            } else {
                log.info("Leadership obtained successfully");
            }
        }).mapEmpty();
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

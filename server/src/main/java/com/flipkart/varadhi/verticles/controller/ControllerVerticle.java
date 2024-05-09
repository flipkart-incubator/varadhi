package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.controller.entities.ConsumerNode;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.ConsumerApiFactory;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.verticles.consumer.ConsumerApiFactoryImpl;
import com.flipkart.varadhi.verticles.webserver.WebServerApiProxy;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.WebServerApi;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final MetaStore metaStore;

    public ControllerVerticle(CoreServices coreServices, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.metaStore = coreServices.getMetaStoreProvider().getMetaStore();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        WebServerApi serverApiProxy = new WebServerApiProxy(messageExchange);
        ConsumerApiFactory consumerApiFactory = new ConsumerApiFactoryImpl(messageExchange);

        ControllerApiMgr controllerApiMgr = new ControllerApiMgr(serverApiProxy, consumerApiFactory, metaStore);
        ControllerApiHandler handler = new ControllerApiHandler(controllerApiMgr, serverApiProxy);

        //TODO::Assuming one controller node for time being. Leader election needs to be added.
        onLeaderElected(controllerApiMgr, handler, messageRouter).onComplete(ar -> {
            if (ar.failed()) {
                startPromise.fail(ar.cause());
            }else{
                startPromise.complete();
            }
        });
    }

    private Future<Void> onLeaderElected(
            ControllerApiMgr controllerApiMgr, ControllerApiHandler handler, MessageRouter messageRouter
    ) {
        // any failures should give up the leadership.
        setupMembershipListener(controllerApiMgr);

        return clusterManager.getAllMembers().onComplete(ar -> {
            if (ar.failed()) {
                log.error("Failed to get all members. Giving up leadership.", ar.cause());
                return;
            }
            List<ConsumerNode> consumerNodes =
                    ar.result().stream().filter(memberInfo -> memberInfo.hasRole(ComponentKind.Consumer))
                            .map(ConsumerNode::new).toList();
            controllerApiMgr.addConsumerNodes(consumerNodes);

            setupApiHandlers(messageRouter, handler);
        }).map(a -> null);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ControllerApiHandler handler) {
        messageRouter.sendHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.sendHandler(ROUTE_CONTROLLER, "stop", handler::stop);
        messageRouter.sendHandler(ROUTE_CONTROLLER, "update", handler::update);
    }

    private void setupMembershipListener(ControllerApiMgr controllerApiMgr) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public void joined(MemberInfo memberInfo) {
                log.debug("Member joined: {}", memberInfo);
                ConsumerNode consumerNode = new ConsumerNode(memberInfo);
                controllerApiMgr.consumerNodeJoined(consumerNode);
            }

            @Override
            public void left(String memberId) {
                log.debug("Member left: {}", memberId);
                controllerApiMgr.consumerNodeLeft(memberId);
            }
        });
    }
}

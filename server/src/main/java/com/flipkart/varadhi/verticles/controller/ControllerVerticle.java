package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.verticles.webserver.WebServerApiProxy;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.WebServerApi;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;

    public ControllerVerticle(AppConfiguration configuration, CoreServices coreServices, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        WebServerApi serverApiProxy = new WebServerApiProxy(messageExchange);
        ControllerApiMgr controllerApiMgr = new ControllerApiMgr(serverApiProxy);
        ControllerApiHandler handler = new ControllerApiHandler(controllerApiMgr, serverApiProxy);
        setupApiHandlers(messageRouter, handler);
        setupMembershipListener(controllerApiMgr);
        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ControllerApiHandler handler) {
        messageRouter.sendHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.sendHandler(ROUTE_CONTROLLER, "stop", handler::stop);
    }

    private void setupMembershipListener(ControllerApiMgr controllerApiMgr) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public void joined(MemberInfo memberInfo) {
                log.debug("Member joined: {}", memberInfo);
                controllerApiMgr.memberJoined(memberInfo);
            }

            @Override
            public void left(String memberId) {
                log.debug("Member left: {}", memberId);
                controllerApiMgr.memberLeft(memberId);
            }
        });
    }
}

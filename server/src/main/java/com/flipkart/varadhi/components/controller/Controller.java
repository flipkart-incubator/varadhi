package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.*;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.components.webserver.WebServerApiProxy;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.WebServerApi;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class Controller implements Component {
    private final VaradhiClusterManager clusterManager;

    public Controller(AppConfiguration configuration, CoreServices coreServices, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public Future<Void> start(Vertx vertx) {
        MessageRouter messageRouter =  clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        WebServerApi serverApiProxy = new WebServerApiProxy(messageExchange);
        ControllerApiMgr controllerApiMgr = new ControllerApiMgr(serverApiProxy);
        ControllerApiHandler handler = new ControllerApiHandler(controllerApiMgr, serverApiProxy);
        setupApiHandlers(messageRouter, handler);
        setupMembershipListener(controllerApiMgr);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> shutdown(Vertx vertx) {
        return Future.succeededFuture();
    }
    private void setupApiHandlers(MessageRouter messageRouter, ControllerApiHandler handler) {
        messageRouter.sendHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.sendHandler(ROUTE_CONTROLLER,"stop", handler::stop);
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

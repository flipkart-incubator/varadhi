package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.ClusterManager;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.ControllerMgr;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.proxies.ServerOpMgrProxy;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class Controller implements Component {

    public Controller(AppConfiguration configuration, CoreServices coreServices) {
    }

    @Override
    public Future<Void> start(Vertx vertx, ClusterManager clusterManager) {
        MessageChannel messageChannel = clusterManager.connect(null);
        setUpMessageHandlers(messageChannel);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> shutdown(Vertx vertx, ClusterManager clusterManager) {
        return Future.succeededFuture();
    }

    private void setUpMessageHandlers(MessageChannel channel) {
        ControllerMgr controllerMgr = new ControllerMgr(new ServerOpMgrProxy(channel));
        SubscriptionOpHandler handler = new SubscriptionOpHandler(controllerMgr, channel);
        channel.register("controller", SubscriptionMessage.class, handler::start);
    }
}

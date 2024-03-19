package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.ClusterManager;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.ControllerMgr;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.ophandlers.WebServerApi;
import com.flipkart.varadhi.core.proxies.WebServerApiProxy;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class Controller implements Component {

    public Controller(AppConfiguration configuration, CoreServices coreServices) {
    }

    @Override
    public Future<Void> start(Vertx vertx, ClusterManager clusterManager) {
        MessageChannel messageChannel = clusterManager.connect(null);
        setupApiHandlers(messageChannel);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> shutdown(Vertx vertx, ClusterManager clusterManager) {
        return Future.succeededFuture();
    }

    private void setupApiHandlers(MessageChannel channel) {
        WebServerApi serverApi = new WebServerApiProxy(channel);
        ControllerMgr controllerMgr = new ControllerMgr(serverApi);
        ControllerApiHandler handler = new ControllerApiHandler(controllerMgr,serverApi);
        channel.register("controller", SubscriptionMessage.class, handler::start);
    }
}

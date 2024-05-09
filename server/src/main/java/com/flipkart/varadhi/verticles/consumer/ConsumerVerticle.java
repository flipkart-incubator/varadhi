package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.impl.ConsumersManagerImpl;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.verticles.controller.ControllerApiProxy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;


public class ConsumerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final String consumerId;
    public ConsumerVerticle(MemberInfo memberInfo, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        consumerId = memberInfo.hostname();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ConsumersManager consumersManager = new ConsumersManagerImpl();

        ControllerApiProxy controllerApiProxy = new ControllerApiProxy(messageExchange);
        ConsumerApiMgr consumerApiManager = new ConsumerApiMgr(consumersManager);
        ConsumerApiHandler handler = new ConsumerApiHandler(consumerApiManager, controllerApiProxy);
        setupApiHandlers(messageRouter, handler);
        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ConsumerApiHandler handler) {
        messageRouter.sendHandler(consumerId, "start", handler::start);
    }
}

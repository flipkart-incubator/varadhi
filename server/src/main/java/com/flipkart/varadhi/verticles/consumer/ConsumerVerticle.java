package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.impl.ConsumersManagerImpl;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.cluster.MemberInfo;
import com.flipkart.varadhi.verticles.controller.ControllerClient;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;


public class ConsumerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final MemberInfo memberInfo;

    public ConsumerVerticle(MemberInfo memberInfo, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.memberInfo = memberInfo;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ConsumersManager consumersManager = new ConsumersManagerImpl(ConsumerInfo.from(memberInfo));

        ControllerClient controllerClient = new ControllerClient(messageExchange);
        ConsumerApiMgr consumerApiManager = new ConsumerApiMgr(consumersManager);
        ConsumerApiHandler handler = new ConsumerApiHandler(consumerApiManager, controllerClient);
        setupApiHandlers(messageRouter, handler);
        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ConsumerApiHandler handler) {
        String consumerId = memberInfo.hostname();
        messageRouter.sendHandler(consumerId, "start", handler::start);
        messageRouter.sendHandler(consumerId, "stop", handler::stop);
        messageRouter.requestHandler(consumerId, "status", handler::status);
        messageRouter.requestHandler(consumerId, "info", handler::info);
    }
}
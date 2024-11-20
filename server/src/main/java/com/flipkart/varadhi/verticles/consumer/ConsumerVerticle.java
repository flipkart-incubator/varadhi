package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.impl.ConsumersManagerImpl;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.cluster.MemberInfo;
import com.flipkart.varadhi.verticles.controller.ControllerConsumerClient;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;


public class ConsumerVerticle extends AbstractVerticle {
    private final VaradhiClusterManager clusterManager;
    private final ConsumerInfo consumerInfo;

    public ConsumerVerticle(MemberInfo memberInfo, VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.consumerInfo = ConsumerInfo.from(memberInfo);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        //TODO:: decide who manages consumerInfo -- ConsumersManagerImpl or ConsumerApiMgr, mostly later.
        ConsumersManager consumersManager = new ConsumersManagerImpl();
        ControllerConsumerClient controllerClient = new ControllerConsumerClient(messageExchange);
        ConsumerApiMgr consumerApiManager = new ConsumerApiMgr(consumersManager, consumerInfo);
        ConsumerApiHandler handler = new ConsumerApiHandler(consumerApiManager, controllerClient);
        setupApiHandlers(messageRouter, handler);
        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private void setupApiHandlers(MessageRouter messageRouter, ConsumerApiHandler handler) {
        String consumerId = consumerInfo.getConsumerId();
        messageRouter.sendHandler(consumerId, "start", handler::start);
        messageRouter.sendHandler(consumerId, "stop", handler::stop);
        messageRouter.sendHandler(consumerId, "unsideline", handler::unsideline);
        messageRouter.requestHandler(consumerId, "status", handler::status);
        messageRouter.requestHandler(consumerId, "info", handler::info);
        messageRouter.requestHandler(consumerId, "getMessages", handler::getMessages);
    }
}

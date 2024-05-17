package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.entities.cluster.ShardRequest;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.verticles.controller.ControllerApiProxy;

import java.util.concurrent.CompletableFuture;


public class ConsumerApiHandler {
    private final ControllerApiProxy controllerApiProxy;
    private final ConsumerApiMgr consumerApiMgr;

    public ConsumerApiHandler(ConsumerApiMgr consumerApiMgr, ControllerApiProxy controllerApiProxy) {
        this.consumerApiMgr = consumerApiMgr;
        this.controllerApiProxy = controllerApiProxy;
    }

    public void start(ClusterMessage message) {
        ShardOperation.StartData startOp = message.getData(ShardOperation.StartData.class);
        consumerApiMgr.start(startOp);
        startOp.markFail("Failed to start subscription");
        controllerApiProxy.update(startOp);
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        ShardRequest request = message.getRequest(ShardRequest.class);
        return consumerApiMgr.getStatus(request.getSubscriptionId(), request.getShardId())
                .thenApply(shardStatus -> message.getResponseMessage(shardStatus));
    }

}

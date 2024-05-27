package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.entities.cluster.ShardRequest;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.verticles.controller.ControllerClient;

import java.util.concurrent.CompletableFuture;


public class ConsumerApiHandler {
    private final ControllerClient controllerClient;
    private final ConsumerApiMgr consumerApiMgr;

    public ConsumerApiHandler(ConsumerApiMgr consumerApiMgr, ControllerClient controllerClient) {
        this.consumerApiMgr = consumerApiMgr;
        this.controllerClient = controllerClient;
    }

    public void start(ClusterMessage message) {
        ShardOperation.StartData startOp = message.getData(ShardOperation.StartData.class);
        consumerApiMgr.start(startOp);
        startOp.markFail("Failed to start subscription");
        controllerClient.update(startOp);
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        ShardRequest request = message.getRequest(ShardRequest.class);
        return consumerApiMgr.getStatus(request.getSubscriptionId(), request.getShardId())
                .thenApply(message::getResponseMessage);
    }

}

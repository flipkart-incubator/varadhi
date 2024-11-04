package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.entities.cluster.ShardStatusRequest;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.verticles.controller.ControllerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;


@Slf4j
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
        failOperation(startOp, "Failed to start subscription");
    }

    public void stop(ClusterMessage message) {
        ShardOperation.StopData stopData = message.getData(ShardOperation.StopData.class);
        consumerApiMgr.stop(stopData);
        failOperation(stopData, "Failed to stop subscription");
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        ShardStatusRequest request = message.getRequest(ShardStatusRequest.class);
        return consumerApiMgr.getShardStatus(request.getSubscriptionId(), request.getShardId())
                .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> info(ClusterMessage message) {
        return consumerApiMgr.getConsumerInfo().thenApply(message::getResponseMessage);
    }

    private void failOperation(ShardOperation.OpData operation, String message) {
        controllerClient.update(operation.getParentOpId(), operation.getOperationId(), ERRORED, message);
    }

}

package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.core.subscription.ShardDlqMessageRequest;
import com.flipkart.varadhi.core.subscription.ShardStatusRequest;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.core.cluster.controller.ControllerConsumerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;


@Slf4j
public class ConsumerApiHandler {
    private final ControllerConsumerClient controllerClient;
    private final ConsumerApiMgr consumerApiMgr;

    public ConsumerApiHandler(ConsumerApiMgr consumerApiMgr, ControllerConsumerClient controllerClient) {
        this.consumerApiMgr = consumerApiMgr;
        this.controllerClient = controllerClient;
    }

    public void start(ClusterMessage message) {
        ShardOperation.StartData startOp = message.getData(ShardOperation.StartData.class);
        consumerApiMgr.start(startOp);
        completeOperation(startOp, "Subscription shard started successfully.");
    }

    public void stop(ClusterMessage message) {
        ShardOperation.StopData stopData = message.getData(ShardOperation.StopData.class);
        consumerApiMgr.stop(stopData);
        completeOperation(stopData, "Subscription shard stopped successfully.");
    }

    public void unsideline(ClusterMessage message) {
        ShardOperation.UnsidelineData unsidelineData = message.getData(ShardOperation.UnsidelineData.class);
        consumerApiMgr.unsideline(unsidelineData);
        failOperation(unsidelineData, "Failed to unsideline messages");
    }

    public CompletableFuture<ResponseMessage> getMessagesByTimestamp(ClusterMessage message) {
        ShardDlqMessageRequest request = message.getData(ShardDlqMessageRequest.class);
        return consumerApiMgr.getMessagesByTimestamp(request.getEarliestFailedAt(), request.getLimit())
                             .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> getMessagesByOffset(ClusterMessage message) {
        ShardDlqMessageRequest request = message.getData(ShardDlqMessageRequest.class);
        return consumerApiMgr.getMessagesByOffset(request.getPageMarker(), request.getLimit())
                             .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        ShardStatusRequest request = message.getRequest(ShardStatusRequest.class);
        return consumerApiMgr.getConsumerState(request.getSubscriptionId(), request.getShardId())
                             .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> info(ClusterMessage message) {
        return consumerApiMgr.getConsumerInfo().thenApply(message::getResponseMessage);
    }

    private void failOperation(ShardOperation.OpData operation, String message) {
        controllerClient.update(operation.getParentOpId(), operation.getOperationId(), ERRORED, message);
    }

    private void completeOperation(ShardOperation.OpData operation, String message) {
        controllerClient.update(operation.getParentOpId(), operation.getOperationId(), COMPLETED, message);
    }

}

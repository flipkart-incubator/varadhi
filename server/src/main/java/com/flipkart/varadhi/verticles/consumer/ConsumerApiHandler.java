package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.messages.ShardMessage;
import com.flipkart.varadhi.consumer.ConsumerApiMgr;
import com.flipkart.varadhi.core.cluster.ShardOperation;
import com.flipkart.varadhi.verticles.controller.ControllerApiProxy;


public class ConsumerApiHandler  {
    private final ControllerApiProxy controllerApiProxy;
    private final ConsumerApiMgr consumerApiMgr;

    public ConsumerApiHandler(ConsumerApiMgr consumerApiMgr, ControllerApiProxy controllerApiProxy) {
        this.consumerApiMgr = consumerApiMgr;
        this.controllerApiProxy = controllerApiProxy;
    }

    public void start(ShardMessage message) {
        ShardOperation.StartData startOp = (ShardOperation.StartData)message.getOperation();
        consumerApiMgr.start( startOp);
        startOp.markFail("Failed to start subscription");
        controllerApiProxy.update(startOp);
    }

}

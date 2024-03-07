package com.flipkart.varadhi.core.proxies;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerOpHandler;

import java.util.concurrent.CompletableFuture;

public class ControllerMgrProxy implements ControllerOpHandler {
    private final String CONTROLLER_ADDR = "Controller";
    private final MessageChannel channel;

    public ControllerMgrProxy(MessageChannel channel) {
        this.channel = channel;
    }

    @Override
    public CompletableFuture<Void> StartSubscription(SubscriptionOperation operation) {
        SubscriptionMessage message = operation.toMessage();
        return channel.send(getAddress(MessageChannel.Method.SEND, message), message);
    }

    private String getAddress(MessageChannel.Method method, SubscriptionMessage message) {
        return CONTROLLER_ADDR + "." + message.getClass().getSimpleName() +  "." + method;
    }
}

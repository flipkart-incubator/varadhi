package com.flipkart.varadhi.core.proxies;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerApi;

import java.util.concurrent.CompletableFuture;

public class ControllerApiProxy implements ControllerApi {
    private final String CONTROLLER_PATH = "controller";
    private final MessageChannel channel;

    public ControllerApiProxy(MessageChannel channel) {
        this.channel = channel;
    }

    @Override
    public CompletableFuture<Void> StartSubscription(SubscriptionOperation.StartData operation) {
        SubscriptionMessage message = new SubscriptionMessage(operation);
        return channel.send(getApiPath(MessageChannel.Method.SEND, message), message);
    }

    private String getApiPath(MessageChannel.Method method, SubscriptionMessage message) {
        return CONTROLLER_PATH + "." + message.getClass().getSimpleName() +  "." + method;
    }
}

package com.flipkart.varadhi.core.proxies;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ServerOpHandler;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.concurrent.CompletableFuture;

public class ServerOpMgrProxy implements ServerOpHandler {
    private final String SERVER_ADDR = "Server";
    private final MessageChannel channel;

    public ServerOpMgrProxy(MessageChannel channel) {
        this.channel = channel;
    }
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation operation) {
        SubscriptionMessage message = operation.toMessage();
        return channel.send(getAddress(MessageChannel.Method.SEND, message), message);
    }

    private String getAddress(MessageChannel.Method method, SubscriptionMessage message) {
        return SERVER_ADDR + "." + message.getClass().getSimpleName() +  "." + method;
    }
}

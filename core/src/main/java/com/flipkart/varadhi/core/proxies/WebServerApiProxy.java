package com.flipkart.varadhi.core.proxies;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class WebServerApiProxy implements WebServerApi {
    private final String SERVER_PATH = "webserver";
    private final MessageChannel channel;

    public WebServerApiProxy(MessageChannel channel) {
        this.channel = channel;
    }
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        SubscriptionMessage message = new SubscriptionMessage(operation);
        return channel.send(getApiPath(MessageChannel.Method.SEND, message), message);
    }
    private String getApiPath(MessageChannel.Method method, SubscriptionMessage message) {
        return SERVER_PATH + "." + message.getClass().getSimpleName() +  "." + method;
    }
}

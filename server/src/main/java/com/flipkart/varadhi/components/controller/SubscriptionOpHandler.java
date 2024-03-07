package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerOpHandler;

import java.util.concurrent.CompletableFuture;

public class SubscriptionOpHandler {
    private final ControllerOpHandler opHandler;
    private final MessageChannel channel;

    public SubscriptionOpHandler(ControllerOpHandler opHandler, MessageChannel channel) {
        this.opHandler = opHandler;
        this.channel = channel;
    }

    public CompletableFuture<Void> start(SubscriptionMessage message) {
        // TODO::it is not async
        SubscriptionOperation operation = message.getOperation();
        if (operation.getKind() != SubscriptionOperation.Kind.START) {
            throw new IllegalArgumentException("Invalid operation type");
        }
        opHandler.StartSubscription(operation).exceptionally(throwable -> {
            operation.setErrorMessage(throwable.getMessage());
            operation.setState(SubscriptionOperation.State.ERRORED);
            channel.send("Server", operation.toMessage());
            return null;
        });
        // send the update to the server.
        return CompletableFuture.completedFuture(null);
    }
}

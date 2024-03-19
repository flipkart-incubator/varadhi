package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.*;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class MessageChannelImpl implements MessageChannel {
    private final EventBus vertxEventBus;
    private final DeliveryOptions deliveryOptions;
    private final Map<String, MessageConsumer<String>> registeredConsumers;

    //TODO:: Add config details to DeliveryOptions e.g. timeouts, tracing etc as part of cluster manager changes.
    public MessageChannelImpl(EventBus vertxEventBus) {
        this.vertxEventBus = vertxEventBus;
        this.deliveryOptions = new DeliveryOptions();
        this.registeredConsumers = new HashMap<>();
    }

    @Override
    public void publish(String apiPath, ClusterMessage msg) {
        throw new NotImplementedException("publish not implemented");
    }

    @Override
    public CompletableFuture<Void> send(String apiPath, ClusterMessage msg) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        vertxEventBus.request(apiPath, JsonMapper.jsonSerialize(msg), deliveryOptions, ar -> {
            if (ar.succeeded()) {
                log.info("received reply: " + ar.result().body());
                future.complete(null);
            } else {
                log.info("send failure: " + ar.cause().getMessage());
                future.completeExceptionally(ar.cause());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ResponseMessage> request(String apiPath, ClusterMessage msg) {
        throw new NotImplementedException("request not implemented");
    }

    @Override
    public void removeMessageHandler(String apiPath) {
        MessageConsumer<String> consumer = registeredConsumers.get(apiPath);
        if (null != consumer) {
            consumer.unregister();
            registeredConsumers.remove(apiPath);
        }
    }

    @Override
    public <E extends ClusterMessage> void register(String basePath, Class<E> messageClazz, SendHandler<E> handler) {
        String apiPath = basePath + "." + messageClazz.getSimpleName() + "." + Method.SEND ;
        MessageConsumer<String> consumer = vertxEventBus.consumer(apiPath, message -> {
            E cm = JsonMapper.jsonDeserialize(message.body(), messageClazz);
            message.reply("received");
            handler.handle(cm);
            log.info("send({}) message processed.", cm.getId());
        });
        // not expected to be registered multiple times.
        registeredConsumers.put(apiPath, consumer);
    }
}

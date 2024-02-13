package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class MessageChannelImpl implements MessageChannel {

    public static final String DELIVERY_KIND_HEADER = "DeliveryKind";
    public static final String DELIVERY_KIND_SEND = "Send";
    public static final String DELIVERY_KIND_REQUEST = "Request";
    public static final String DELIVERY_KIND_PUBLISH = "Publish";

    private final EventBus vertxEventBus;
    private final DeliveryOptions deliveryOptions;
    private final Map<String, MessageConsumer<String>> registerdConsumers;

    //TODO:: Add config details to DeliveryOptions e.g. timeouts, tracing etc as part of cluster manager changes.
    public MessageChannelImpl(Vertx vertx) {
        this.vertxEventBus = vertx.eventBus();
        this.deliveryOptions = new DeliveryOptions();
        this.registerdConsumers = new HashMap<>();
    }

    @Override
    public void publish(String address, ClusterMessage msg) {
        throw new NotImplementedException("publish not implemented");
    }

    @Override
    public CompletableFuture<Void> send(String address, ClusterMessage msg) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        DeliveryOptions deliveryOptions = getDeliveryOptions(DELIVERY_KIND_SEND);
        vertxEventBus.request(address, JsonMapper.jsonSerialize(msg), deliveryOptions, ar -> {
            if (ar.succeeded()) {
                log.info("Received reply: " + ar.result().body());
                future.complete(null);
            } else {
                log.info("send failure: " + ar.cause().getMessage());
                future.completeExceptionally(ar.cause());
            }
        });
        return future;
    }

    private DeliveryOptions getDeliveryOptions(String deliveryKind) {
        // Add other static config options like timeout etc.
        return new DeliveryOptions(deliveryOptions).addHeader(DELIVERY_KIND_HEADER, deliveryKind);
    }

    @Override
    public CompletableFuture<ResponseMessage> request(String address, ClusterMessage msg) {
        throw new NotImplementedException("request not implemented");
    }

    @Override
    public void removeMessageHandler(String address) {
        MessageConsumer<String> consumer = registerdConsumers.get(address);
        if (null != consumer) {
            consumer.unregister();
            registerdConsumers.remove(address);
        }
    }

    @Override
    public void addMessageHandler(String address, MessageHandler messageHandler) {
        //TODO:: Evaluate if dispatcher mechanism is needed to control the execution parallelism.
        MessageConsumer<String> consumer = vertxEventBus.consumer(address, message -> {
            ClusterMessage clusterMessage = JsonMapper.jsonDeserialize(message.body(), ClusterMessage.class);
            if (null == message.replyAddress()) {
                // received via publish message
                messageHandler.handle(clusterMessage);
                log.info("publish({}) message processed.", clusterMessage.getId());
            } else {
                String deliveryKind = message.headers().get(DELIVERY_KIND_HEADER);
                if (DELIVERY_KIND_SEND.equals(deliveryKind)) {
                    // received via send message
                    // acknowledge to indicate that message is delivered, processing is async.
                    message.reply("Received");
                    messageHandler.handle(clusterMessage);
                    log.info("send({}) message processed.", clusterMessage.getId());
                } else {
                    // received via request message
                    messageHandler.request(clusterMessage).handle((response, failure) -> {
                        if (null != failure) {
                            message.fail(500, failure.getMessage());
                            log.error("request({}) processing failed. {}", clusterMessage.getId(), failure.getMessage());
                        } else {
                            message.reply(JsonMapper.jsonSerialize(response));
                            log.info("request({}) processed with response({})", clusterMessage.getId(), response.getId());
                        }
                        return null;
                    });
                }
            }
        });
        // not expected to be registered multiple times.
        registerdConsumers.put(address, consumer);
    }
}

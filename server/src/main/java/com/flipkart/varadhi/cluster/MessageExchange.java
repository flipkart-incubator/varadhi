package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * This is a subset of similar methods from vertx.EventBus for messages exchange between nodes.
 * Supported methods are
 * send(): dispatch message to one of handler registered at <routeName>.<apiName>.send, delivery can be tracked using the future.
 * request(): dispatch message to one of handler registered at <routeName>.<apiName>.request and wait for a response.
 * publish(): would dispatch message to all handler registered at <routeName>.<apiName>.publish
 */
@Slf4j
public class MessageExchange {
    EventBus vertxEventBus;
    DeliveryOptions deliveryOptions;

    public MessageExchange(EventBus vertxEventBus, DeliveryOptions deliveryOptions) {
        this.vertxEventBus = vertxEventBus;
        this.deliveryOptions = deliveryOptions;
    }

    public CompletableFuture<Void> send(String routeName, String apiName, ClusterMessage msg) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String apiPath = getPath(routeName, apiName, RouteMethod.SEND);
        try {
            vertxEventBus.request(apiPath, JsonMapper.jsonSerialize(msg), deliveryOptions, ar -> {
                if (ar.succeeded()) {
                    log.debug("send({}, {}) delivered successfully.", apiPath, msg.getId());
                    future.complete(null);
                } else {
                    log.error("send({}, {}) failed: {}.", apiPath, msg.getId(), ar.cause().getMessage());
                    future.completeExceptionally(ar.cause());
                }
            });
            return future;
        } catch (Exception e) {
            log.error("send({}, {}) Unexpected failure:{}", apiPath, msg.getId(), e.getMessage());
            return CompletableFuture.failedFuture(e);
        }
    }

    public void publish(String routeName, String apiName, ClusterMessage msg) {
        String apiPath = getPath(routeName, apiName, RouteMethod.PUBLISH);
        try {
            vertxEventBus.publish(apiPath, JsonMapper.jsonSerialize(msg), deliveryOptions);
            log.debug("publish({}, {}) delivered.", apiPath, msg.getId());
        } catch (Exception e) {
            log.error("publish({}, {}) Unexpected failure:{}", apiPath, msg.getId(), e.getMessage());
        }
    }

    public CompletableFuture<ResponseMessage> request(String routeName, String apiName, ClusterMessage msg) {
        CompletableFuture<ResponseMessage> future = new CompletableFuture<>();
        String apiPath = getPath(routeName, apiName, RouteMethod.REQUEST);
        try {
            vertxEventBus.request(apiPath, JsonMapper.jsonSerialize(msg), deliveryOptions, ar -> {
                if (ar.succeeded()) {
                    log.debug("request({}, {}) delivered. {}.", apiPath, msg.getId(), ar.result().body());
                    ResponseMessage response =
                            JsonMapper.jsonDeserialize((String) ar.result().body(), ResponseMessage.class);
                    if (response.getException() != null) {
                        future.completeExceptionally(response.getException());
                    } else {
                        future.complete(response);
                    }
                } else {
                    log.error("request({}, {}) failure: {}.", apiPath, msg.getId(), ar.cause().getMessage());
                    future.completeExceptionally(ar.cause());
                }
            });
            return future;
        } catch (Exception e) {
            log.error("request({}, {}) Unexpected failure:{}", apiPath, msg.getId(), e.getMessage());
            return CompletableFuture.failedFuture(e);
        }
    }

    private String getPath(String routeName, String apiName, RouteMethod method) {
        return String.format("%s.%s.%s", routeName, apiName, method);
    }
}

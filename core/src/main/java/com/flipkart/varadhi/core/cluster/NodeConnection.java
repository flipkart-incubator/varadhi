package com.flipkart.varadhi.core.cluster;

import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

/**
 * This is subset of methods from vertx.EventBus which are node specific. Instead of using address concept, we will
 * rely on path concept from http. address is not needed as this class is meant to represent a connection to a specific
 * node.
 * publish methods are fire & forget.
 * send methods don't have any response, but their delivery can be tracked using the future.
 * request methods are for traditional request-response pattern.
 *
 * TODO: whether to use the Message class. It is unlikely we will use reply-to-reply feature.
 */
public interface NodeConnection {

    NodeInfo getNodeInfo();

    void publish(String path, Object msg);

    void publish(String path, Object msg, DeliveryOptions options);

    Future<Void> send(String path, Object msg);

    Future<Void> send(String path, Object msg, DeliveryOptions options);

    <T> Future<Message<T>> request(String path, Object msg);

    <T> Future<Message<T>> request(String path, Object msg, DeliveryOptions options);
}

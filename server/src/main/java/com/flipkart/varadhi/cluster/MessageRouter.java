package com.flipkart.varadhi.cluster;


import com.flipkart.varadhi.cluster.messages.*;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * API handler registration for message exchange between nodes. This is a subset of similar methods from
 * vertx.EventBus
 * It uses concept of routeName, apiName and method to identify a specific api handler.
 * routeName -- name for a message route (e.g. controller, webserver, consumer-node-1).
 * apiName -- name of the method e.g. start/stop/update etc.
 * method -- either send, publish or request
 * Address at which api handler is registered would be <routeName>.<apiNamme>.<method> e.g. controller.start.send
 *
 * publish methods are fire & forget.
 * send methods don't have any response, but their delivery can be tracked using the future.
 * request methods are for traditional request-response pattern.
 */

@Slf4j
public class MessageRouter {

    private final EventBus vertxEventBus;
    private final DeliveryOptions deliveryOptions;


    public MessageRouter(EventBus vertxEventBus, DeliveryOptions deliveryOptions) {
        this.vertxEventBus = vertxEventBus;
        this.deliveryOptions = deliveryOptions;
    }

    public <E extends ClusterMessage> void sendHandler(String routeName, String apiName, SendHandler<E> handler) {
        String apiPath = getApiPath(routeName, apiName, RouteMethod.SEND);
        vertxEventBus.consumer(apiPath, message -> {
            E cm = (E) JsonMapper.jsonDeserialize((String) message.body(), ClusterMessage.class);
            log.debug("Received msg via - send({}, {})", apiPath, cm.getId());
            handler.handle(cm); // this is async invocation.
            message.reply("received", deliveryOptions);
        });
    }

    public <E extends ClusterMessage, R extends ResponseMessage> void requestHandler(
            String routeName, String apiName, RequestHandler<E, R> handler
    ) {
        throw new NotImplementedException("handleRequest not implemented");
    }

    public <E extends ClusterMessage> void publishHandler(String routeName, String apiName, PublishHandler<E> handler) {
        throw new NotImplementedException("handlePublish not implemented");
    }


    private String getApiPath(String routeName, String apiName, RouteMethod method) {
        return String.format("%s.%s.%s", routeName, apiName, method);
    }
}

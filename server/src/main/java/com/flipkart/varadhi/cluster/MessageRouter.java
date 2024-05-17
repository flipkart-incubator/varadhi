package com.flipkart.varadhi.cluster;


import com.flipkart.varadhi.cluster.messages.*;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

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

    public void sendHandler(String routeName, String apiName, MsgHandler handler) {
        String apiPath = getApiPath(routeName, apiName, RouteMethod.SEND);
        vertxEventBus.consumer(apiPath, message -> {
            ClusterMessage msg =  JsonMapper.jsonDeserialize((String) message.body(), ClusterMessage.class);
            log.debug("Received msg via - send({}, {})", apiPath, msg.getId());
            handler.handle(msg); // this is async invocation.
            message.reply("received ok", deliveryOptions);
        });
    }

    public void requestHandler(String routeName, String apiName, RequestHandler handler
    ) {
        String apiPath = getApiPath(routeName, apiName, RouteMethod.REQUEST);
        vertxEventBus.consumer(apiPath, message -> {
            ClusterMessage msg = JsonMapper.jsonDeserialize((String) message.body(), ClusterMessage.class);
            log.debug("Received msg via - request({}, {})", apiPath, msg.getId());
            handler.handle(msg).thenAccept( response ->  message.reply(JsonMapper.jsonSerialize(response), deliveryOptions)); // this is async invocation.
        });
    }

    public void publishHandler(String routeName, String apiName, MsgHandler handler) {
        throw new NotImplementedException("handlePublish not implemented");
    }

    private String getApiPath(String routeName, String apiName, RouteMethod method) {
        return String.format("%s.%s.%s", routeName, apiName, method);
    }
}

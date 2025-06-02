package com.flipkart.varadhi.cluster;


import com.flipkart.varadhi.cluster.messages.*;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.entities.JsonMapper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

/**
 * API handler registration for message exchange between nodes. This is a subset of similar methods from
 * vertx.EventBus
 * It uses concept of routeName, apiName and method to identify a specific api handler.
 * routeName -- name for a message route (e.g. controller, webserver, consumer-node-1).
 * apiName -- name of the method e.g. start/stop/update etc.
 * method -- either send, publish or request
 * Address at which api handler is registered would be <routeName>.<apiName>.<method> e.g. controller.start.send
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
            ClusterMessage msg = JsonMapper.jsonDeserialize((String)message.body(), ClusterMessage.class);
            log.debug("Received msg via - send({}, {})", apiPath, msg.getId());
            try {
                // this is async invocation.
                handler.handle(msg);
            } catch (Exception e) {
                log.error("send handler.handle({}) Unhandled exception: {}", message.body(), e.getMessage());
            } finally {
                // Send ensures only delivery and not execution.
                // Client will not be aware of any kind of failure either in invocation or in execution of the
                // message's send handler
                message.reply("received ok", deliveryOptions);
            }
        });
    }

    public void requestHandler(String routeName, String apiName, RequestHandler handler) {
        String apiPath = getApiPath(routeName, apiName, RouteMethod.REQUEST);
        vertxEventBus.consumer(apiPath, message -> {
            ClusterMessage msg = JsonMapper.jsonDeserialize((String)message.body(), ClusterMessage.class);
            log.debug("Received msg via - request({}, {})", apiPath, msg.getId());
            try {
                handler.handle(msg)
                       .thenAccept(response -> message.reply(JsonMapper.jsonSerialize(response), deliveryOptions))
                       .exceptionally(t -> {
                           log.error("request handler completed exceptionally: {}", t.getMessage());
                           //TODO::exception exchange is not working always. ser(de) is failing for some exception.
                           Exception failure;
                           if (t instanceof ExecutionException) {
                               failure = (ExecutionException)t.getCause();
                           } else if (t instanceof Exception) {
                               failure = (Exception)t;
                           } else {
                               failure = new VaradhiException(t);
                           }
                           ResponseMessage response = msg.getResponseMessage(failure);
                           message.reply(JsonMapper.jsonSerialize(response), deliveryOptions);
                           return null;
                       });
            } catch (Exception e) {
                log.error("request handler Unhandled exception: {}", e.getMessage());
                ResponseMessage response = msg.getResponseMessage(e);
                message.reply(JsonMapper.jsonSerialize(response), deliveryOptions);
            }
        });
    }

    public void publishHandler(String routeName, String apiName, MsgHandler handler) {
        throw new UnsupportedOperationException("handlePublish not implemented");
    }

    private String getApiPath(String routeName, String apiName, RouteMethod method) {
        return String.format("%s.%s.%s", routeName, apiName, method);
    }
}

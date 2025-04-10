package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.events.EntityEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public final class EntityEventApiHandler {
    private final CompositeEntityEventHandler cacheEventHandler;

    public EntityEventApiHandler(CompositeEntityEventHandler cacheEventHandler) {
        this.cacheEventHandler = cacheEventHandler;
    }

    public CompletableFuture<ResponseMessage> handleEvent(ClusterMessage message) {
        try {
            EntityEvent event = message.getData(EntityEvent.class);

            if (log.isDebugEnabled()) {
                log.debug("Processing entity cache event: {}", event);
            }

            cacheEventHandler.handleEvent(event);
            return CompletableFuture.completedFuture(ResponseMessage.fromPayload("OK", message.getId()));
        } catch (Exception e) {
            log.error("Failed to process entity cache event from message: {}", message.getId(), e);
            return CompletableFuture.completedFuture(message.getResponseMessage(e));
        }
    }
}

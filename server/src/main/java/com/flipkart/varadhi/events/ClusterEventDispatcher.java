package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.core.cluster.EventListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Dispatches entity events received via cluster messages to the appropriate event listeners.
 * <p>
 * This class acts as a bridge between the cluster messaging system and the entity event processing system,
 * extracting entity events from cluster messages and dispatching them to the appropriate event listener.
 */
@Slf4j
public final class ClusterEventDispatcher {
    private final EventListener eventListener;

    /**
     * Creates a new ClusterEventDispatcher with the specified event listener.
     *
     * @param eventListener the listener to process entity events
     * @throws NullPointerException if eventListener is null
     */
    public ClusterEventDispatcher(EventListener eventListener) {
        this.eventListener = Objects.requireNonNull(eventListener, "Event handler cannot be null");
    }

    /**
     * Dispatches an entity event from a cluster message to the appropriate event listener.
     * <p>
     * This method extracts the entity event from the message, dispatches it to the event listener,
     * and returns an appropriate response message.
     *
     * @param message the cluster message containing the entity event
     * @return a future that completes with the response message
     */
    public CompletableFuture<ResponseMessage> handleEvent(ClusterMessage message) {
        if (message == null) {
            log.error("Received null message");
            return CompletableFuture.completedFuture(
                ResponseMessage.fromException(new IllegalArgumentException("Message cannot be null"), null)
            );
        }

        try {
            EntityEvent<?> event = message.getData(EntityEvent.class);
            if (event == null) {
                log.error("Failed to extract EntityEvent from message: {}", message.getId());
                return CompletableFuture.completedFuture(
                    message.getResponseMessage(new IllegalArgumentException("Invalid event data"))
                );
            }

            log.debug("Dispatching {} event for {} {}", event.operation(), event.resourceType(), event.resourceName());

            eventListener.processEvent(event);
            return CompletableFuture.completedFuture(ResponseMessage.fromPayload("OK", message.getId()));
        } catch (Exception e) {
            log.error("Failed to dispatch entity event from message: {}", message.getId(), e);
            return CompletableFuture.completedFuture(message.getResponseMessage(e));
        }
    }
}

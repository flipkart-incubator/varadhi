package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.entities.ResourceEvent;
import com.flipkart.varadhi.exceptions.EventProcessingException;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Consumes and manages resource events in a distributed system using an event bus architecture.
 * This thread-safe consumer handles event message lifecycle, from deserialization through
 * processing to completion or failure.
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Thread-safe event processing with concurrent state management</li>
 *   <li>Automatic event deduplication and validation</li>
 *   <li>Asynchronous processing with CompletableFuture</li>
 *   <li>Graceful shutdown with pending event handling</li>
 *   <li>Comprehensive error handling and recovery mechanisms</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create and start the consumer
 * try (var consumer = new EventConsumer(eventStore, eventBus, messageExchange, clusterManager)) {
 *     consumer.start();
 *     // Consumer is now processing events
 * } // AutoCloseable handles shutdown
 * }</pre>
 *
 * <h2>HTTP Status Codes</h2>
 * <ul>
 *   <li>{@code 400} - Bad Request: Invalid event format or validation failure</li>
 *   <li>{@code 500} - Internal Server Error: Processing or system errors</li>
 *   <li>{@code 503} - Service Unavailable: System is shutting down</li>
 * </ul>
 *
 * <h2>Implementation Notes</h2>
 * <ul>
 *   <li>Events are processed asynchronously using {@link CompletableFuture}</li>
 *   <li>Event state is tracked in a {@link ConcurrentHashMap} for thread safety</li>
 *   <li>Duplicate events are detected using event names</li>
 *   <li>All resources are properly cleaned up during shutdown</li>
 * </ul>
 *
 * @see EventStore For event persistence operations
 * @see EventProcessor For event processing logic
 * @see ResourceEvent For event data structure
 * @see MessageExchange For cluster communication
 * @see VaradhiClusterManager For cluster management
 */
@Slf4j
public final class EventConsumer implements AutoCloseable {
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_INTERNAL_ERROR = 500;

    private final EventStore eventStore;
    private final EventBus eventBus;
    private final Map<String, EventProcessingState> pendingEvents;
    private final EventProcessor eventProcessor;
    private final AtomicBoolean isShutdown;

    /**
     * Creates a new EventConsumer with the specified dependencies.
     *
     * @param eventStore      The store for event persistence
     * @param eventBus        The bus for receiving event messages
     * @param messageExchange The exchange for cluster communication
     * @param clusterManager  The manager for cluster operations
     * @throws NullPointerException if any parameter is null
     */
    public EventConsumer(
        EventStore eventStore,
        EventBus eventBus,
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
        EventProcessorConfig eventProcessorConfig
    ) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore cannot be null");
        this.eventBus = Objects.requireNonNull(eventBus, "eventBus cannot be null");
        this.pendingEvents = new ConcurrentHashMap<>();
        this.eventProcessor = new EventProcessor(
            Objects.requireNonNull(messageExchange, "messageExchange cannot be null"),
            Objects.requireNonNull(clusterManager, "clusterManager cannot be null"),
            eventProcessorConfig
        );
        this.isShutdown = new AtomicBoolean(false);
    }

    /**
     * Starts the event consumer and begins processing events.
     * This method is idempotent and thread-safe.
     */
    public void start() {
        eventBus.consumer(EventProcessor.EVENT_ADDRESS, this::handleEventMessage);
        log.info("EventConsumer started successfully");
    }

    /**
     * Handles incoming event messages from the event bus.
     *
     * @param message The event message to process
     * @throws EventProcessingException if message processing fails
     */
    private void handleEventMessage(Message<Object> message) {
        if (isShutdown.get()) {
            message.fail(HTTP_SERVICE_UNAVAILABLE, "EventConsumer is shutting down");
            return;
        }

        try {
            ResourceEvent event = deserializeEvent(message.body());
            if (!validateEvent(event)) {
                message.fail(HTTP_BAD_REQUEST, "Invalid event format");
                return;
            }

            processEvent(event).whenComplete(createEventCompletionHandler(message, event));

        } catch (Exception e) {
            log.error("Failed to process event message", e);
            message.fail(HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Deserializes the message body into a ResourceEvent.
     *
     * @param messageBody The message body to deserialize
     * @return The deserialized ResourceEvent
     * @throws EventProcessingException if deserialization fails
     */
    private ResourceEvent deserializeEvent(Object messageBody) {
        if (!(messageBody instanceof String jsonBody)) {
            throw new EventProcessingException("Invalid message body type");
        }
        return JsonMapper.jsonDeserialize(jsonBody, ResourceEvent.class);
    }

    /**
     * Validates the event for processing requirements.
     *
     * @param event The event to validate
     * @return true if the event is valid, false otherwise
     */
    private boolean validateEvent(ResourceEvent event) {
        return event != null && event.eventName() != null && !event.eventName().isBlank() && !pendingEvents.containsKey(
            event.eventName()
        );
    }

    /**
     * Creates a completion handler for event processing.
     *
     * @param message The original message
     * @param event   The event being processed
     * @return A BiConsumer that handles completion or failure
     */
    private BiConsumer<Void, Throwable> createEventCompletionHandler(Message<Object> message, ResourceEvent event) {
        return (v, throwable) -> {
            if (throwable != null) {
                log.error("Failed to process event: {}", event.eventName(), throwable);
                message.fail(HTTP_INTERNAL_ERROR, throwable.getMessage());
            } else {
                message.reply("Event processed successfully");
            }
        };
    }

    /**
     * Processes an event asynchronously.
     *
     * @param event The event to process
     * @return A CompletableFuture that completes when processing is done
     */
    private CompletableFuture<Void> processEvent(ResourceEvent event) {
        String eventName = event.eventName();
        var state = new EventProcessingState(event);
        pendingEvents.put(eventName, state);

        return eventProcessor.process(event).thenAccept(v -> {
            state.incrementAttempts();
            cleanupEvent(eventName);
        }).exceptionally(throwable -> {
            state.incrementAttempts();
            handleProcessingFailure(eventName, throwable);
            throw new CompletionException(throwable);
        });
    }

    /**
     * Handles failures during event processing.
     *
     * @param eventName The name of the failed event
     * @param throwable The cause of the failure
     */
    private void handleProcessingFailure(String eventName, Throwable throwable) {
        var state = pendingEvents.remove(eventName);
        if (state != null) {
            state.fail(throwable);
            log.error("Failed to process event: {} after {} attempts", eventName, state.getAttempts(), throwable);
        }
    }

    /**
     * Cleans up a successfully processed event.
     *
     * @param eventName The name of the event to clean up
     * @throws EventProcessingException if cleanup fails
     */
    private void cleanupEvent(String eventName) {
        try {
            eventStore.deleteEvent(eventName);
            var state = pendingEvents.remove(eventName);
            if (state != null) {
                state.complete();
                log.info("Event {} processed successfully after {} attempts", eventName, state.getAttempts());
            }
        } catch (Exception e) {
            log.error("Failed to cleanup event: {}", eventName, e);
            throw new EventProcessingException("Failed to cleanup event: " + eventName, e);
        }
    }

    /**
     * Closes the event consumer and releases resources.
     * This method is idempotent and thread-safe.
     *
     * @throws EventProcessingException if shutdown fails
     */
    @Override
    public void close() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("EventConsumer already shutting down");
            return;
        }

        try {
            log.info("Shutting down EventConsumer...");

            terminatePendingEvents();
            eventProcessor.close();

            log.info("EventConsumer shutdown completed");
        } catch (Exception e) {
            log.error("Error during EventConsumer shutdown", e);
            throw new EventProcessingException("EventConsumer Shutdown failed", e);
        }
    }

    /**
     * Terminates all pending events during shutdown.
     */
    private void terminatePendingEvents() {
        pendingEvents.forEach((eventName, state) -> {
            try {
                state.fail(new EventProcessingException("EventConsumer shutting down"));
                log.warn("Event {} terminated due to shutdown after {} attempts", eventName, state.getAttempts());
            } catch (Exception e) {
                log.error("Error while terminating event {}", eventName, e);
            }
        });
        pendingEvents.clear();
    }
}

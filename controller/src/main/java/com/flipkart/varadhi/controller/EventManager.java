package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.events.ExponentialBackoff;
import com.flipkart.varadhi.controller.events.StateResolutionResult;
import com.flipkart.varadhi.entities.EventMarker;
import com.flipkart.varadhi.entities.ResourceEvent;
import com.flipkart.varadhi.exceptions.EventProcessingException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.utils.JsonMapper;
import com.google.common.collect.Lists;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manages resource events in a distributed system using ZooKeeper for event coordination
 * and Vert.x EventBus for event distribution. This class provides reliable event processing
 * with ordered delivery, retry mechanisms, and graceful shutdown capabilities.
 *
 * <h2>Core Responsibilities</h2>
 * <ul>
 *   <li>Watches for resource events in ZooKeeper using CuratorCache</li>
 *   <li>Maintains event ordering using sequence numbers</li>
 *   <li>Resolves current resource state from MetaStore</li>
 *   <li>Publishes events to EventBus for cluster-wide distribution</li>
 *   <li>Handles event batching and retry logic</li>
 * </ul>
 *
 * <h2>Event Processing Flow</h2>
 * <ol>
 *   <li>Event created in ZooKeeper triggers CuratorCache listener</li>
 *   <li>Event is queued if processing pending events, otherwise processed immediately</li>
 *   <li>Current resource state is resolved from MetaStore</li>
 *   <li>Event is published to EventBus with exponential backoff retry</li>
 * </ol>
 *
 * <h2>Configuration Constants</h2>
 * <ul>
 *   <li>BATCH_SIZE: Number of events processed in a batch (default: 100)</li>
 *   <li>MAX_PUBLISH_RETRIES: Maximum retry attempts for publishing (default: 3)</li>
 *   <li>PUBLISH_RETRY_BASE_DELAY: Base delay between retries (default: 100ms)</li>
 *   <li>SHUTDOWN_TIMEOUT: Maximum wait time during shutdown (default: 5s)</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * try (EventManager manager = new EventManager(eventStore, metaStore, eventBus)) {
 *     // Manager automatically starts processing events
 *
 *     // Check health status
 *     if (manager.isHealthy()) {
 *         // System is processing events
 *     }
 * } // AutoCloseable handles graceful shutdown
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li>Uses single-threaded executor for event processing</li>
 *   <li>Maintains thread safety using volatile flags and concurrent collections</li>
 *   <li>Handles interrupts appropriately during shutdown</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <ul>
 *   <li>Failed events are logged and can be retried</li>
 *   <li>Exponential backoff for publish retries</li>
 *   <li>Graceful handling of initialization failures</li>
 *   <li>Proper resource cleanup during shutdown</li>
 * </ul>
 *
 * @see EventStore For event persistence and cache operations
 * @see MetaStore For resource state resolution
 * @see EventBus For event distribution
 * @see CuratorCache For ZooKeeper event watching
 * @see EventMarker For event metadata
 * @see ResourceEvent For resolved event data
 */
@Slf4j
public class EventManager implements AutoCloseable {
    private static final String EVENT_ADDRESS = "varadhi.events";
    private static final int BATCH_SIZE = 100;
    private static final int MAX_PUBLISH_RETRIES = 3;
    private static final Duration PUBLISH_RETRY_BASE_DELAY = Duration.ofMillis(100);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
    private static final String THREAD_NAME_PREFIX = "event-manager-";
    private static final String EVENT_NAME_PREFIX = "event_";

    private final EventStore eventStore;
    private final MetaStore metaStore;
    private final CuratorCache curatorCache;
    private final EventBus eventBus;
    private final ExecutorService executor;
    private final BlockingQueue<EventMarker> eventQueue;
    private final CompletableFuture<Void> initializationFuture;

    private volatile boolean processingPendingEvents = true;
    private volatile boolean isShutdown = false;

    /**
     * Constructs a new EventManager instance.
     * Initializes the event processing infrastructure and starts watching for events.
     *
     * @param eventStore The store for event persistence and cache operations
     * @param metaStore  The store for resource state resolution
     * @param eventBus   The event bus for distributing events across the cluster
     * @throws NullPointerException if any parameter is null
     */
    public EventManager(EventStore eventStore, MetaStore metaStore, EventBus eventBus) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore cannot be null");
        this.metaStore = Objects.requireNonNull(metaStore, "metaStore cannot be null");
        this.eventBus = Objects.requireNonNull(eventBus, "eventBus cannot be null");
        this.curatorCache = (CuratorCache)eventStore.getEventCache();
        this.executor = createExecutor();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.initializationFuture = new CompletableFuture<>();

        initialize();
    }

    /**
     * Creates a virtual thread executor for event processing.
     * Uses a single thread to ensure ordered event processing with uncaught exception handling.
     *
     * @return A new ExecutorService configured with virtual threads
     */
    private ExecutorService createExecutor() {
        return Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name(THREAD_NAME_PREFIX, 0).uncaughtExceptionHandler((t, e) -> {
                log.error("Uncaught exception in thread {}", t.getName(), e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }).factory()
        );
    }

    /**
     * Initializes the event manager asynchronously.
     * Sets up event listeners, starts cache, and begins processing pending events.
     */
    private void initialize() {
        CompletableFuture.runAsync(() -> {
            try {
                setupEventListener();
                curatorCache.start();

                processPendingEvents();

                startEventProcessing();

                log.info("EventManager initialization completed successfully");
                initializationFuture.complete(null);
            } catch (Exception e) {
                initializationFuture.completeExceptionally(e);
                log.error("Failed to initialize EventManager", e);
            }
        }, executor);
    }

    /**
     * Extracts the sequence number from an event name.
     * Event names follow the format "event_[sequence]".
     *
     * @param eventName The name of the event
     * @return The sequence number
     * @throws EventProcessingException if the event name format is invalid
     */
    private long extractSequenceNumber(String eventName) {
        try {
            return Long.parseLong(eventName.substring(EVENT_NAME_PREFIX.length()));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.error("Invalid event name format: {}", eventName);
            throw new EventProcessingException("Invalid event name format: " + eventName, e);
        }
    }

    /**
     * Processes pending events in batches with ordered delivery.
     * Events are sorted by sequence number before processing.
     *
     * @throws EventProcessingException if processing fails
     */
    private void processPendingEvents() {
        try {
            List<EventMarker> pendingEvents = eventStore.getPendingEvents();
            log.info("Processing {} pending events", pendingEvents.size());

            pendingEvents.sort(Comparator.comparingLong(event -> extractSequenceNumber(event.getName())));

            Lists.partition(pendingEvents, BATCH_SIZE).forEach(this::processBatch);

            processingPendingEvents = false;
            log.info("Completed processing all pending events");
        } catch (Exception e) {
            log.error("Failed to process pending events", e);
            throw new EventProcessingException("Failed to process pending events", e);
        }
    }

    /**
     * Processes a batch of events.
     * Handles individual event failures without affecting the batch.
     *
     * @param batch List of events to process
     */
    private void processBatch(List<EventMarker> batch) {
        batch.forEach(event -> {
            try {
                processEvent(event);
            } catch (Exception e) {
                log.error("Failed to process event in batch: {}", event, e);
            }
        });
    }

    /**
     * Sets up the event listener for ZooKeeper cache changes.
     * Configures listener to handle event creation notifications.
     */
    private void setupEventListener() {
        CuratorCacheListener listener = CuratorCacheListener.builder().forCreates(this::handleEventCreated).build();

        curatorCache.listenable().addListener(listener);
    }

    /**
     * Handles event creation notifications from ZooKeeper.
     * Either queues or processes the event based on system state.
     *
     * @param data The created event data
     */
    private void handleEventCreated(ChildData data) {
        CompletableFuture.runAsync(() -> {
            try {
                var event = JsonMapper.jsonDeserialize(data.getData(), EventMarker.class);
                if (processingPendingEvents) {
                    queueEvent(event);
                } else {
                    processEvent(event);
                }
            } catch (Exception e) {
                log.error("Failed to handle created event: {}", data.getPath(), e);
                throw new EventProcessingException("Failed to handle created event", e);
            }
        }, executor);
    }

    /**
     * Queues an event for later processing.
     * Uses a blocking queue with timeout to prevent indefinite blocking.
     *
     * @param event The event to queue
     * @throws EventProcessingException if queueing is interrupted
     */
    private void queueEvent(EventMarker event) {
        try {
            if (!eventQueue.offer(event, 1, TimeUnit.SECONDS)) {
                eventQueue.put(event);
            }
            if (log.isDebugEnabled()) {
                log.debug("Successfully queued event: {}", event);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventProcessingException("Interrupted while queueing event", e);
        }
    }

    /**
     * Starts asynchronous processing of queued events.
     * Continues processing until shutdown is initiated.
     */
    private void startEventProcessing() {
        CompletableFuture.runAsync(this::processQueuedEvents, executor).exceptionally(throwable -> {
            log.error("Event queue processing failed", throwable);
            return null;
        });
    }

    /**
     * Processes events from the queue.
     * Handles interrupts and exceptions while maintaining continuous processing.
     */
    private void processQueuedEvents() {
        while (!isShutdown) {
            try {
                EventMarker event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    processEvent(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Event processing interrupted", e);
                break;
            } catch (Exception e) {
                log.error("Error processing event from queue", e);
            }
        }
    }

    /**
     * Processes a single event.
     * Resolves the current resource state and publishes with retry.
     *
     * @param event The event to process
     */
    private void processEvent(EventMarker event) {
        try {
            var resolvedEvent = resolveResourceEvent(event);
            publishEventWithRetry(resolvedEvent);

            if (log.isDebugEnabled()) {
                log.debug("Successfully processed event: {}", event);
            }
        } catch (EventProcessingException e) {
            log.error("Failed to process event: {}", event, e);
        }
    }

    /**
     * Resolves a resource event from an event marker.
     * Determines current resource state and operation type.
     *
     * @param event The event marker to resolve
     * @return The resolved resource event
     * @throws EventProcessingException if resolution fails
     */
    private ResourceEvent resolveResourceEvent(EventMarker event) {
        try {
            var result = resolveCurrentState(event);
            return ResourceEvent.of(
                event.getName(),
                event.getResourceType(),
                event.getResourceName(),
                result.operation(),
                result.state()
            );
        } catch (Exception e) {
            throw new EventProcessingException(String.format("Failed to resolve resource event: %s", event), e);
        }
    }

    /**
     * Resolves the current state of a resource.
     * Handles different resource types and state resolution failures.
     *
     * @param event The event containing resource information
     * @return The state resolution result
     */
    private StateResolutionResult resolveCurrentState(EventMarker event) {
        return switch (event.getResourceType()) {
            case PROJECT -> safeResolveState(() -> metaStore.getProject(event.getResourceName()));
            case TOPIC -> safeResolveState(() -> metaStore.getTopic(event.getResourceName()));
            case SUBSCRIPTION -> safeResolveState(() -> metaStore.getSubscription(event.getResourceName()));
            case ROOT, ORG, TEAM, IAM_POLICY -> StateResolutionResult.noStateRequired();
        };
    }

    /**
     * Safely resolves resource state with exception handling.
     * Converts not found exceptions to appropriate state results.
     *
     * @param stateSupplier The supplier function for state resolution
     * @return The state resolution result
     */
    private StateResolutionResult safeResolveState(Supplier<Object> stateSupplier) {
        try {
            Object state = stateSupplier.get();
            return StateResolutionResult.of(state);
        } catch (ResourceNotFoundException e) {
            return StateResolutionResult.notFound();
        }
    }

    /**
     * Publishes an event with retry capability.
     * Uses exponential backoff for retries on failure.
     *
     * @param event The event to publish
     * @throws EventProcessingException if publishing fails after max retries
     */
    private void publishEventWithRetry(ResourceEvent event) {
        var retryPolicy = new ExponentialBackoff(MAX_PUBLISH_RETRIES, PUBLISH_RETRY_BASE_DELAY);

        while (retryPolicy.shouldRetry()) {
            try {
                eventBus.publish(EVENT_ADDRESS, JsonMapper.jsonSerialize(event));
                log.debug("Published resolved event: {}", event);
                return;
            } catch (Exception e) {
                retryPolicy.onFailure(e, event);
            }
        }
    }

    /**
     * Checks if the event manager is healthy and processing events.
     * Verifies initialization status and component health.
     *
     * @return true if the manager is healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            return !isShutdown && initializationFuture.isDone() && !initializationFuture.isCompletedExceptionally()
                   && curatorCache.listenable() != null && !executor.isShutdown() && !executor.isTerminated();
        } catch (Exception e) {
            log.warn("CuratorCache health check failed", e);
            return false;
        }
    }

    /**
     * Drains remaining events from the queue during shutdown.
     * Attempts to process all queued events before shutting down.
     */
    private void drainEventQueue() {
        log.info("Draining event queue. Remaining events: {}", eventQueue.size());
        EventMarker event;
        while ((event = eventQueue.poll()) != null) {
            try {
                processEvent(event);
            } catch (Exception e) {
                log.error("Failed to process event during shutdown: {}", event, e);
            }
        }
    }

    /**
     * Closes the event manager and releases resources.
     * Ensures graceful shutdown with timeout handling.
     *
     * @throws Exception if shutdown fails or is interrupted
     */
    @Override
    public void close() {
        if (isShutdown) {
            return;
        }

        log.info("Initiating EventManager shutdown...");
        isShutdown = true;

        try {
            initializationFuture.get(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            drainEventQueue();

            executor.shutdown();
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }

            curatorCache.close();
            log.info("EventManager shutdown completed");
        } catch (Exception e) {
            log.error("Error during EventManager shutdown", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

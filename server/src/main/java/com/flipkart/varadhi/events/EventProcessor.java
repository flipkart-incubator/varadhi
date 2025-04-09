package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.exceptions.EventProcessingException;
import com.flipkart.varadhi.controller.DefaultMetaStoreChangeListener;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.spi.db.MetaStore;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The EventProcessor is responsible for distributing entity events to all nodes in the cluster
 * and tracking their completion status. It uses virtual threads for improved performance and
 * scalability in handling concurrent event processing across multiple nodes.
 * <p>
 * This class implements a distributed event processing system where:
 * <ol>
 *     <li>Events are distributed to all active cluster nodes</li>
 *     <li>Each node processes events in its own virtual thread</li>
 *     <li>A committer thread tracks completion status and completes futures when all nodes have processed an event</li>
 *     <li>The system handles node joins/leaves dynamically</li>
 * </ol>
 */
@Slf4j
public final class EventProcessor implements EntityEventListener {

    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final AtomicBoolean isShutdown;

    private final Map<String, BlockingQueue<EventWrapper>> nodeEventQueues;
    private final Map<String, Thread> nodeVirtualThreads;

    private final BlockingQueue<EventWrapper> inFlightEvents;
    private Thread committerThread;

    /**
     * Creates and initializes a new EventProcessor.
     *
     * @param messageExchange      The message exchange for inter-node communication
     * @param clusterManager       The cluster manager to track node membership
     * @param metaStore            The metastore to register for entity events
     * @param eventProcessorConfig Configuration for the event processor
     * @return A future that completes when the EventProcessor is fully initialized
     */
    public static Future<EventProcessor> create(
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
        MetaStore metaStore,
        EventProcessorConfig eventProcessorConfig
    ) {
        Promise<EventProcessor> promise = Promise.promise();
        EventProcessor processor = new EventProcessor(messageExchange, eventProcessorConfig);

        processor.setupMembershipListener(clusterManager);

        clusterManager.getAllMembers().compose(initialMembers -> {
            processor.initializeEventQueuesAndThreads(initialMembers);

            if (initialMembers.isEmpty()) {
                log.warn("No members found in cluster, processor will wait for members to join");
            }

            processor.initialize(metaStore);
            processor.startCommitterThread();

            return Future.succeededFuture(processor);
        }).onComplete(ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                processor.close();
                log.error("Failed to initialize EventProcessor", ar.cause());
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    /**
     * Initializes the EventProcessor with the provided MetaStore.
     * Registers a listener for entity events and sets the shutdown flag to false.
     *
     * @param metaStore The metastore to register for entity events
     */
    private void initialize(MetaStore metaStore) {
        isShutdown.set(false);
        metaStore.registerEventListener(new DefaultMetaStoreChangeListener(metaStore, this));
    }

    /**
     * Creates a new EventProcessor instance with the given message exchange and configuration.
     * This constructor initializes the internal data structures but does not start any threads.
     *
     * @param messageExchange      The message exchange for inter-node communication
     * @param eventProcessorConfig Configuration for the event processor, or default if null
     */
    private EventProcessor(MessageExchange messageExchange, EventProcessorConfig eventProcessorConfig) {
        this.messageExchange = messageExchange;
        this.eventProcessorConfig = eventProcessorConfig != null ?
            eventProcessorConfig :
            EventProcessorConfig.getDefault();
        this.isShutdown = new AtomicBoolean(true);
        this.nodeEventQueues = new ConcurrentHashMap<>();
        this.nodeVirtualThreads = new ConcurrentHashMap<>();
        this.inFlightEvents = new LinkedBlockingQueue<>();
    }

    /**
     * Initializes event queues and virtual threads for all initial cluster members.
     * Each member gets its own event queue and virtual thread for processing events.
     *
     * @param members List of initial cluster members
     */
    private void initializeEventQueuesAndThreads(List<MemberInfo> members) {
        if (members.isEmpty()) {
            log.info("No initial members to initialize");
            return;
        }

        for (MemberInfo member : members) {
            String hostname = member.hostname();

            if (nodeEventQueues.containsKey(hostname)) {
                log.debug("Node {} already initialized, skipping", hostname);
                continue;
            }

            log.info("Creating event queue and virtual thread for node: {}", hostname);

            BlockingQueue<EventWrapper> queue = new LinkedBlockingQueue<>();
            nodeEventQueues.put(hostname, queue);

            startNodeVirtualThread(member, queue);
        }

        log.info("Initialized event queues and virtual threads, total active nodes: {}", nodeEventQueues.size());
    }

    /**
     * Creates and starts a virtual thread for processing events for a specific node.
     * The thread is named according to the configuration and the node's hostname.
     *
     * @param member The member information for the node
     * @param queue  The event queue for the node
     */
    private void startNodeVirtualThread(MemberInfo member, BlockingQueue<EventWrapper> queue) {
        String hostname = member.hostname();
        Thread virtualThread = Thread.ofVirtual()
                                     .name(eventProcessorConfig.getNodeProcessorThreadName(hostname))
                                     .start(() -> processEventsForNode(member, queue, hostname));

        nodeVirtualThreads.put(hostname, virtualThread);
    }

    /**
     * Main processing loop for a node's virtual thread.
     * Takes events from the node's queue, processes them, and marks them as complete.
     * Continues until shutdown is requested or the thread is interrupted.
     *
     * @param member   The member information for the node
     * @param queue    The event queue for the node
     * @param hostname The hostname of the node
     */
    private void processEventsForNode(MemberInfo member, BlockingQueue<EventWrapper> queue, String hostname) {
        log.info("Started virtual thread for node: {}", hostname);

        try {
            while (!isShutdown.get()) {
                try {
                    EventWrapper eventWrapper = queue.take();
                    log.debug("Processing {} event for node {}", eventWrapper.event.resourceName(), hostname);

                    processEventWithRetry(eventWrapper, member);

                    eventWrapper.markNodeComplete(hostname);
                    log.debug("Node {} completed processing {} event", hostname, eventWrapper.event.resourceName());
                } catch (InterruptedException e) {
                    log.info("Virtual thread for node {} was interrupted", hostname);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in virtual thread for node {}", hostname, e);
                }
            }
        } finally {
            log.info("Virtual thread for node {} is shutting down", hostname);
        }
    }

    /**
     * Processes an event for a node with retry logic.
     * Attempts to process the event until successful or until shutdown/interruption.
     * Uses exponential backoff between retry attempts.
     *
     * @param eventWrapper The event wrapper containing the event to process
     * @param member       The member information for the node
     */
    private void processEventWithRetry(EventWrapper eventWrapper, MemberInfo member) {
        EntityEvent<?> event = eventWrapper.event;
        boolean success = false;
        int attempts = 0;
        final String nodeId = member.hostname();

        while (shouldContinue() && !success) {
            attempts++;
            success = attemptProcessEvent(event, nodeId, attempts);

            if (!success && shouldContinue()) {
                try {
                    performBackoffAndSleep(event, nodeId, attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.info("Retry interrupted for event {} on node {}", event.resourceName(), nodeId);
                    break;
                }
            }
        }

        if (!success && shouldContinue()) {
            log.warn("Event {} processing for node {} exited retry loop without success", event.resourceName(), nodeId);
        }
    }

    /**
     * Checks if processing should continue based on shutdown state and thread interruption status.
     *
     * @return true if processing should continue, false otherwise
     */
    private boolean shouldContinue() {
        return !isShutdown.get() && !Thread.currentThread().isInterrupted();
    }

    /**
     * Attempts to process an event for a node.
     * Sends a request to the node and handles the response.
     *
     * @param event    The event to process
     * @param nodeId   The ID of the node
     * @param attempts The current attempt number
     * @return true if processing was successful, false otherwise
     */
    private boolean attemptProcessEvent(EntityEvent<?> event, String nodeId, int attempts) {
        try {
            log.debug("Processing event {} for node {} (attempt {})", event.resourceName(), nodeId, attempts);

            ClusterMessage message = ClusterMessage.of(event);
            ResponseMessage response = messageExchange.request("resource-events", "process-event", message)
                                                      .orTimeout(
                                                          eventProcessorConfig.getClusterMemberTimeoutMs(),
                                                          TimeUnit.MILLISECONDS
                                                      )
                                                      .get();

            if (response.getException() != null) {
                throw response.getException();
            }

            log.debug(
                "Successfully processed event {} for node {} on attempt {}",
                event.resourceName(),
                nodeId,
                attempts
            );
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Event processing interrupted for {} on node {}", event.resourceName(), nodeId);
            return false;
        } catch (Exception e) {
            if (shouldContinue()) {
                log.debug("Error processing event {} for node {}: {}", event.resourceName(), nodeId, e.getMessage());
            }
            return false;
        }
    }

    /**
     * Performs backoff before retrying event processing.
     * Uses exponential backoff with a maximum delay.
     *
     * @param event    The event being processed
     * @param nodeId   The ID of the node
     * @param attempts The current attempt number
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    private void performBackoffAndSleep(EntityEvent<?> event, String nodeId, int attempts) throws InterruptedException {
        Duration backoff = calculateBackoffDuration(attempts);

        log.warn(
            "Failed to process event {} for node {} (attempt {}), retrying in {} ms",
            event.resourceName(),
            nodeId,
            attempts,
            backoff.toMillis()
        );

        Thread.sleep(backoff);
    }

    /**
     * Calculates the backoff duration for retry attempts using exponential backoff.
     * The duration increases exponentially with each attempt, up to a maximum value.
     *
     * @param attempt The current attempt number
     * @return The calculated backoff duration
     */
    private Duration calculateBackoffDuration(int attempt) {
        long delayMs = Math.min(
            eventProcessorConfig.getRetryDelayMs() * (long)Math.pow(
                2,
                Math.min(eventProcessorConfig.getMaxBackoffAttempts(), attempt - 1)
            ),
            eventProcessorConfig.getMaxBackoffMs()
        );
        return Duration.ofMillis(delayMs);
    }

    /**
     * Creates and starts the committer thread as a virtual thread.
     * The committer thread is responsible for tracking event completion across all nodes
     * and completing the associated futures.
     */
    private void startCommitterThread() {
        committerThread = Thread.ofVirtual()
                                .name(eventProcessorConfig.getEventCommitterThreadName())
                                .start(this::runCommitterLoop);
    }

    /**
     * Main processing loop for the committer thread.
     * Processes events that are ready for completion and handles exceptions.
     * Continues until shutdown is requested or the thread is interrupted.
     */
    private void runCommitterLoop() {
        log.info("Started event committer thread");

        try {
            while (!isShutdown.get()) {
                try {
                    processNextCompletableEvent();
                } catch (InterruptedException e) {
                    log.info("Committer thread was interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in committer thread", e);
                }
            }
        } finally {
            handleRemainingEventsOnShutdown();
        }
    }

    /**
     * Processes the next completable event from the queue.
     * Waits for the event to be ready (completed by all nodes) before completing its future.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void processNextCompletableEvent() throws InterruptedException {
        EventWrapper eventWrapper = inFlightEvents.take();

        while (!isEventReady(eventWrapper) && !isShutdown.get()) {
            boolean signaled = eventWrapper.awaitReady(eventProcessorConfig.getEventReadyCheckMs());

            if (!signaled && !eventWrapper.isCompleteForAllNodes() && !isShutdown.get()) {
                log.trace(
                    "Waiting for event {} to complete: {}/{} nodes done",
                    eventWrapper.event.resourceName(),
                    eventWrapper.getCompletedNodeCount(),
                    eventWrapper.getTotalNodeCount()
                );
            }
        }

        if (isShutdown.get()) {
            eventWrapper.completeFuture.completeExceptionally(
                new EventProcessingException("Processing interrupted due to shutdown")
            );
            return;
        }

        log.debug("Event {} processed by all nodes, completing", eventWrapper.event.resourceName());
        eventWrapper.completeFuture.complete(null);
    }

    /**
     * Checks if an event is ready for completion.
     * An event is ready when it has been processed by all nodes.
     *
     * @param eventWrapper The event wrapper to check
     * @return true if the event is ready for completion, false otherwise
     */
    private boolean isEventReady(EventWrapper eventWrapper) {
        return eventWrapper.isCompleteForAllNodes();
    }

    /**
     * Handles remaining events during shutdown.
     * Completes all in-flight events with exceptions to indicate shutdown.
     */
    private void handleRemainingEventsOnShutdown() {
        log.info("Event committer thread is shutting down");

        inFlightEvents.forEach(
            wrapper -> wrapper.completeFuture.completeExceptionally(
                new EventProcessingException("EventProcessor is shutting down")
            )
        );

        inFlightEvents.clear();
    }

    /**
     * Sets up a membership listener to handle node joins and leaves.
     * When a node joins, a new event queue and virtual thread are created for it.
     * When a node leaves, its events are marked as complete and its thread is interrupted.
     *
     * @param clusterManager The cluster manager to add the membership listener to
     */
    private void setupMembershipListener(VaradhiClusterManager clusterManager) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(MemberInfo memberInfo) {
                String hostname = memberInfo.hostname();
                log.info("New member joined: {}", hostname);

                BlockingQueue<EventWrapper> queue = new LinkedBlockingQueue<>();
                nodeEventQueues.put(hostname, queue);

                startNodeVirtualThread(memberInfo, queue);

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String hostname) {
                log.info("Member left: {}", hostname);

                inFlightEvents.forEach(eventWrapper -> {
                    if (eventWrapper.nodes.contains(hostname)) {
                        eventWrapper.markNodeComplete(hostname);
                    }
                });

                Thread virtualThread = nodeVirtualThreads.remove(hostname);
                if (virtualThread != null) {
                    virtualThread.interrupt();
                }

                nodeEventQueues.remove(hostname);

                return CompletableFuture.completedFuture(null);
            }
        });
    }

    /**
     * Handles entity change events by distributing them to all nodes.
     * Creates a CompletableFuture that will be completed when all nodes have processed the event.
     *
     * @param event The entity event to process
     * @throws IllegalStateException if the EventProcessor has been shut down
     */
    @Override
    public void onChange(EntityEvent<?> event) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Change Listener has been stopped");
        }
        CompletableFuture<Void> completeFuture = new CompletableFuture<>();
        enqueueEventToAllNodes(event, completeFuture);
    }

    /**
     * Enqueues an event to all active nodes for processing.
     * Creates an EventWrapper to track the event's processing status across all nodes.
     * If no nodes are available, completes the future exceptionally.
     *
     * @param event          The entity event to enqueue
     * @param completeFuture The future to complete when processing is done
     */
    private void enqueueEventToAllNodes(EntityEvent<?> event, CompletableFuture<Void> completeFuture) {
        synchronized (nodeEventQueues) {
            Set<String> currentNodes = Set.copyOf(nodeEventQueues.keySet());

            if (currentNodes.isEmpty()) {
                completeFuture.completeExceptionally(
                    new EventProcessingException("No nodes available to process the event")
                );
                return;
            }

            EventWrapper eventWrapper = new EventWrapper(event, currentNodes, completeFuture);

            inFlightEvents.add(eventWrapper);

            for (String hostname : currentNodes) {
                BlockingQueue<EventWrapper> nodeQueue = nodeEventQueues.get(hostname);
                if (nodeQueue != null) {
                    nodeQueue.add(eventWrapper);
                } else {
                    eventWrapper.markNodeComplete(hostname);
                }
            }

            log.debug("Enqueued event {} to {} nodes", event.resourceName(), currentNodes.size());
        }
    }

    /**
     * Shuts down the EventProcessor and all its threads.
     * Interrupts all threads, waits for them to complete, and clears all queues.
     * This method is idempotent and can be called multiple times safely.
     */
    public void close() {
        if (isShutdown.compareAndSet(false, true)) {
            log.info("Shutting down EventProcessor");

            try {
                if (committerThread != null) {
                    committerThread.interrupt();
                    committerThread.join(eventProcessorConfig.getThreadJoinTimeoutPrimaryMs());
                }

                nodeVirtualThreads.forEach((hostname, thread) -> {
                    log.debug("Interrupting virtual thread for node: {}", hostname);
                    thread.interrupt();
                });

                for (Thread thread : nodeVirtualThreads.values()) {
                    thread.join(eventProcessorConfig.getThreadJoinTimeoutWorkerMs());
                }

                nodeEventQueues.clear();
                nodeVirtualThreads.clear();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for threads to shut down", e);
                Thread.currentThread().interrupt();
            }

            log.info("EventProcessor shutdown complete");
        }
    }
}

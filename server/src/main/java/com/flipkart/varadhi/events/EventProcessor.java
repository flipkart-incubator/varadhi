package com.flipkart.varadhi.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.Extensions;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.exceptions.EventProcessingException;
import com.flipkart.varadhi.controller.DefaultMetaStoreChangeListener;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.spi.db.MetaStore;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.vertx.core.Future;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

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
@ExtensionMethod ({Extensions.LockExtensions.class})
public final class EventProcessor implements EntityEventListener {

    public static final String EVENT_SENDER_TASK_NAME_FORMAT = "event-sender-%s";
    public static final String EVENT_COMMITTER_TASK_NAME = "event-committer";

    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final AtomicBoolean isShutdown;

    private final Map<String, BlockingQueue<EventWrapper>> memberEventQueues;
    private final Map<String, Thread> memberEventSenders;
    private final Lock membershipChangeLock;

    private final BlockingQueue<EventWrapper> inFlightEvents;
    private final Thread eventCommitterTask;

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
        EventProcessor processor = new EventProcessor(messageExchange, eventProcessorConfig);

        processor.setupMembershipListener(clusterManager);

        return clusterManager.getAllMembers().compose(initialMembers -> {
            processor.setupInitialMember(initialMembers);

            // all initialization has happened. Start listening to the entity events
            metaStore.registerEventListener(new DefaultMetaStoreChangeListener(metaStore, processor));
            return Future.succeededFuture(processor);
        });
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
        this.isShutdown = new AtomicBoolean(false);
        this.memberEventQueues = new ConcurrentHashMap<>();
        this.memberEventSenders = new ConcurrentHashMap<>();
        this.membershipChangeLock = new ReentrantLock();
        this.inFlightEvents = new LinkedBlockingQueue<>();
        this.eventCommitterTask = startCommitterTask();
    }

    /**
     * Creates and starts the committer thread as a virtual thread.
     * The committer thread is responsible for tracking event completion across all nodes
     * and completing the associated futures.
     */
    private Thread startCommitterTask() {
        return Thread.ofVirtual().name(EVENT_COMMITTER_TASK_NAME).start(new EventCommitter());
    }

    /**
     * Creates and starts a virtual thread for processing events for a specific node.
     * The thread is named according to the configuration and the node's hostname.
     *
     * @param member The member information for the node
     */
    private Thread startEventSenderTaskForMember(MemberInfo member) {
        String hostname = member.hostname();
        return Thread.ofVirtual()
                     .name(EVENT_SENDER_TASK_NAME_FORMAT.formatted(hostname))
                     .start(new EventSender(member));
    }

    /**
     * Initializes event queues and virtual threads for all initial cluster members.
     * Each member gets its own event queue and virtual thread for processing events.
     *
     * @param members List of initial cluster members
     */
    private void setupInitialMember(List<MemberInfo> members) {
        for (MemberInfo member : members) {
            initializeNewMember(member);
        }
        log.info("Initialization of the initial members are done. Initial members count: {}", members.size());
    }

    private void initializeNewMember(MemberInfo memberInfo) {
        String hostname = memberInfo.hostname();
        log.info("New member joined: {}", hostname);

        membershipChangeLock.lockAndRun(() -> {
            if (memberEventQueues.get(hostname) != null) {
                log.warn("The host: {} already is registered. Ignoring the event", hostname);
            } else {
                memberEventQueues.put(hostname, new LinkedBlockingQueue<>());
                memberEventSenders.put(hostname, startEventSenderTaskForMember(memberInfo));
            }
        });
    }

    private void cleanupMember(String hostname) {
        /*
         * Fine to run on the listener thread, as the expectation is that the lock contention will be minimal.
         */

        log.info("Member left: {}", hostname);

        Thread task = memberEventSenders.remove(hostname);
        if (task != null) {
            task.interrupt();
            // we need a metric to track the running tasks
        } else {
            log.warn("host {} has left, but the event sender task was not present to cleanup", hostname);
        }

        var queue = membershipChangeLock.lockAndSupply(() -> memberEventQueues.remove(hostname));

        if (queue == null) {
            log.warn("host {} has left, but the event queue was not present to cleanup", hostname);
        } else {
            queue.forEach(eventWrapper -> eventWrapper.markNodeAsNonParticipant(hostname));
            queue.clear();
        }
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
                initializeNewMember(memberInfo);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String hostname) {
                cleanupMember(hostname);
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
        handle(event);
    }

    /**
     * Enqueues an event to all active nodes for processing.
     * Creates an EventWrapper to track the event's processing status across all nodes.
     * If no nodes are available, completes the future exceptionally.
     *
     * @param event          The entity event to enqueue
     */
    private void handle(EntityEvent<?> event) {
        CompletableFuture<Void> promise = new CompletableFuture<>().thenAccept(r -> {
            event.markAsProcessed();
        });

        membershipChangeLock.lockAndRun(() -> {
            Set<String> currentNodes = Set.copyOf(memberEventQueues.keySet());

            if (currentNodes.isEmpty()) {
                log.warn("No nodes available to process the event {} {}", event.resourceType(), event.resourceName());
                promise.complete(null);
                return;
            }

            EventWrapper eventWrapper = new EventWrapper(event, currentNodes, promise);
            inFlightEvents.add(eventWrapper);

            for (String hostname : currentNodes) {
                BlockingQueue<EventWrapper> nodeQueue = memberEventQueues.get(hostname);
                assert nodeQueue != null : "because we are holding the lock";
                nodeQueue.add(eventWrapper);
            }

            log.debug("Enqueued event {} to {} nodes", event.resourceName(), currentNodes.size());
        });
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
                // force the pending events to fail
                if (eventCommitterTask != null) {
                    eventCommitterTask.interrupt();
                    eventCommitterTask.join(eventProcessorConfig.getTasksJoinTimeoutMs());
                }

                memberEventSenders.forEach((hostname, thread) -> {
                    log.debug("Interrupting virtual thread for node: {}", hostname);
                    thread.interrupt();
                });

                for (Thread thread : memberEventSenders.values()) {
                    thread.join(eventProcessorConfig.getTasksJoinTimeoutMs());
                }

                memberEventSenders.clear();
                membershipChangeLock.lockAndRun(memberEventQueues::clear);

            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for threads to shut down", e);
                Thread.currentThread().interrupt();
            }

            log.info("EventProcessor shutdown complete");
        }
    }

    /**
     * Main processing loop to send events to cluster members.
     * Takes events from the member's queue, processes them, and marks them as complete.
     * Continues until shutdown is requested or the thread is interrupted.
     */
    class EventSender implements Runnable {
        private final MemberInfo member;
        private final RetryPolicy<String> retryPolicy;

        public EventSender(MemberInfo member) {
            this.member = member;
            this.retryPolicy = RetryPolicy.<String>builder()
                                          .withBackoff(
                                              eventProcessorConfig.getRetryBackoff(),
                                              eventProcessorConfig.getMaxRetryBackoff()
                                          )
                                          .abortOn(InterruptedException.class)
                                          .abortIf((result, failure) -> failure != null && isShutdown.get())
                                          .onRetry(
                                              event -> log.warn(
                                                  "Attempt #{} failed. Retrying task 'sendEntity' for host: {}. Last Exception: {}",
                                                  event.getAttemptCount(),
                                                  member.hostname(),
                                                  event.getLastException().getMessage()
                                              )
                                          )
                                          .build();
        }


        @Override
        public void run() {
            final String hostname = member.hostname();
            final BlockingQueue<EventWrapper> queue = memberEventQueues.get(hostname);

            if (queue == null) {
                throw new IllegalStateException("event queue was not initialized for host: " + hostname);
            }

            log.info("Started event sender for host: {}", hostname);

            try {
                while (!isShutdown.get()) {
                    EventWrapper eventWrapper = queue.take();
                    log.debug("Sending {} event to host {}", eventWrapper.event.resourceName(), hostname);
                    String response = Failsafe.with(retryPolicy).get(() -> sendEvent(eventWrapper.event, hostname));
                    eventWrapper.markNodeComplete(hostname);
                    log.debug(
                        "Received response: {} for {} event from host {}",
                        response,
                        eventWrapper.event.resourceName(),
                        hostname
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Event sender for host {} was interrupted", hostname);
            } catch (Exception e) {
                log.error("Event sender failed for host {}", hostname, e);
                throw new IllegalStateException(e);
            } finally {
                log.info("Event sender for host {} is shutting down", hostname);
            }
        }

        /**
         * Attempts to process an event for a node.
         * Sends a request to the node and handles the response.
         *
         * @param event    The event to process
         * @param hostname The ID of the node
         * @return string response from the node
         */
        private String sendEvent(EntityEvent<?> event, String hostname) throws Exception {

            ClusterMessage message = ClusterMessage.of(event);
            ResponseMessage response = messageExchange.request(hostname, "entity-events", message)
                                                      .orTimeout(
                                                          eventProcessorConfig.getClusterMemberTimeout().toMillis(),
                                                          TimeUnit.MILLISECONDS
                                                      )
                                                      .get();
            if (response.getException() != null) {
                throw response.getException();
            }

            // TODO: enhance the response object later. using string as standin for now.
            return response.getResponse(String.class);
        }
    }


    /**
     * Main processing loop for the committer thread.
     * Processes events that are ready for completion and handles exceptions.
     * Continues until shutdown is requested or the thread is interrupted.
     */
    class EventCommitter implements Runnable {

        @Override
        public void run() {
            log.info("Started event committer task");

            try {
                while (!isShutdown.get()) {
                    tryEventCommit();
                }
            } catch (InterruptedException e) {
                log.info("Committer thread was interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error in committer thread", e);
            } finally {
                failPendingEvents();
            }
        }

        /**
         * Processes the next completable event from the queue.
         * Waits for the event to be ready (completed by all nodes) before completing its future.
         *
         * @throws InterruptedException if the thread is interrupted while waiting
         */
        private void tryEventCommit() throws InterruptedException {
            EventWrapper eventWrapper = inFlightEvents.take();

            while (!eventWrapper.isCompleteForAllNodes() && !isShutdown.get()) {
                boolean completed = eventWrapper.awaitComplete();

                if (!completed && !isShutdown.get()) {
                    log.trace(
                        "Waiting for event {} to complete: {}/{} nodes done",
                        eventWrapper.event.resourceName(),
                        eventWrapper.getCompletedNodeCount(),
                        eventWrapper.getTotalNodeCount()
                    );
                }
            }

            if (isShutdown.get()) {
                eventWrapper.completeExceptionally(
                    new EventProcessingException("Processing interrupted due to shutdown")
                );
                return;
            }

            log.info("Event {} processed by all nodes, completing", eventWrapper.event.resourceName());
            eventWrapper.completeSuccessfully();
        }

        /**
         * Handles remaining events during shutdown.
         * Completes all in-flight events with exceptions to indicate shutdown.
         */
        private void failPendingEvents() {
            log.info("Event committer thread is shutting down. Failing inFlight requests");

            // create a temp list, so that we fail only those which we have drained. iterating and clearing
            // might clear events which we may not have failed.
            List<EventWrapper> tempList = new ArrayList<>();
            inFlightEvents.drainTo(tempList);
            for (var e : tempList) {
                e.completeExceptionally(new EventProcessingException("EventProcessor is shutting down"));
            }
        }
    }


    /**
     * Wrapper for entity events that tracks processing status across cluster nodes.
     * <p>
     * This class provides thread-safe tracking of which nodes have completed processing
     * an event and signals when all nodes have finished.
     */
    static class EventWrapper {
        /**
         * The entity event being processed.
         */
        final EntityEvent<?> event;

        /**
         * Set of node hostnames that need to process this event.
         */
        private final Set<String> participantHosts;

        /**
         * Set of node hostnames that have completed processing this event.
         */
        private final Set<String> completedHosts;

        /**
         * Future that completes when all nodes have processed the event.
         */
        private final CompletableFuture<Void> promise;

        /**
         * Lock for thread-safe operations and condition signaling.
         */
        private final ReentrantLock lock;

        /**
         * Condition variable for signaling when an event is ready.
         */
        private final Condition readyCondition;

        /**
         * Creates a new event wrapper for the given event and set of nodes.
         *
         * @param event                 The entity event to be processed
         * @param participantHosts      The set of nodes that need to process this event
         * @param promise               The future to complete when all nodes have processed the event
         * @throws NullPointerException if any parameter is null
         */
        public EventWrapper(EntityEvent<?> event, Set<String> participantHosts, CompletableFuture<Void> promise) {
            this.event = Objects.requireNonNull(event, "Event cannot be null");
            this.promise = Objects.requireNonNull(promise, "promise cannot be null");

            // Create thread-safe sets for nodes
            this.participantHosts = ConcurrentHashMap.newKeySet();
            this.participantHosts.addAll(participantHosts);
            this.completedHosts = ConcurrentHashMap.newKeySet();

            // Initialize lock and condition
            this.lock = new ReentrantLock();
            this.readyCondition = lock.newCondition();
        }

        /**
         * Marks a node as having completed processing this event.
         * <p>
         * If all nodes have completed processing, signals any waiting threads.
         *
         * @param hostname The hostname of the node that completed processing
         */
        public void markNodeComplete(String hostname) {
            lock.lock();
            try {
                completedHosts.add(hostname);
                if (isCompleteForAllNodes()) {
                    readyCondition.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        public void markNodeAsNonParticipant(String hostname) {
            lock.lock();
            try {
                participantHosts.remove(hostname);
                if (isCompleteForAllNodes()) {
                    readyCondition.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Checks if all nodes have completed processing this event.
         *
         * @return true if all nodes have completed processing, false otherwise
         */
        boolean isCompleteForAllNodes() {
            return !participantHosts.isEmpty() && completedHosts.containsAll(participantHosts);
        }

        /**
         * Returns the number of nodes that have completed processing this event.
         *
         * @return the count of nodes that have completed processing
         */
        public int getCompletedNodeCount() {
            return completedHosts.size();
        }

        /**
         * Returns the total number of nodes that need to process this event.
         *
         * @return the total count of nodes
         */
        public int getTotalNodeCount() {
            return participantHosts.size();
        }

        /**
         * Waits until either all nodes have completed processing or the specified timeout elapses.
         * <p>
         * This method must be called while not holding the lock, as it will acquire the lock internally.
         *
         * @return true if the event is complete or the thread was signaled, false if the waiting time elapsed
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean awaitComplete() throws InterruptedException {
            lock.lock();
            try {
                if (isCompleteForAllNodes()) {
                    return true;
                }
                readyCondition.await();
                return isCompleteForAllNodes();
            } finally {
                lock.unlock();
            }
        }

        public void completeExceptionally(Throwable t) {
            promise.completeExceptionally(t);
        }

        public void completeSuccessfully() {
            promise.complete(null);
        }
    }
}

package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.exceptions.EventProcessingException;
import com.flipkart.varadhi.controller.EntityEventProcessor;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class EventProcessor implements EntityEventProcessor {

    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final AtomicBoolean isShutdown;

    private final Map<String, BlockingQueue<EventWrapper>> nodeEventQueues;
    private final Map<String, Thread> nodeVirtualThreads;

    private final BlockingQueue<EventWrapper> inFlightEvents;
    private Thread committerThread;

    public static Future<EventProcessor> create(
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
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

            processor.initialize();
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

    private void initialize() {
        isShutdown.set(false);
    }

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

    private void initializeEventQueuesAndThreads(List<MemberInfo> members) {
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
        log.info("Initialized node queues and virtual threads, total active nodes: {}", nodeEventQueues.size());
    }

    private void startNodeVirtualThread(MemberInfo member, BlockingQueue<EventWrapper> queue) {
        String hostname = member.hostname();
        Thread virtualThread = Thread.ofVirtual()
                                     .name(eventProcessorConfig.getNodeProcessorThreadName(hostname))
                                     .start(() -> {
                                         log.info("Started virtual thread for node: {}", hostname);

                                         while (!isShutdown.get()) {
                                             try {
                                                 EventWrapper eventWrapper = queue.take();
                                                 log.debug(
                                                     "Processing {} event for node {}",
                                                     eventWrapper.event.resourceName(),
                                                     hostname
                                                 );

                                                 processEventWithRetry(eventWrapper, member);

                                                 eventWrapper.markNodeComplete(hostname);
                                                 log.debug(
                                                     "Node {} completed processing {} event",
                                                     hostname,
                                                     eventWrapper.event.resourceName()
                                                 );
                                             } catch (InterruptedException e) {
                                                 log.info("Virtual thread for node {} was interrupted", hostname);
                                                 Thread.currentThread().interrupt();
                                                 break;
                                             } catch (Exception e) {
                                                 log.error("Error in virtual thread for node {}", hostname, e);
                                             }
                                         }
                                         log.info("Virtual thread for node {} is shutting down", hostname);
                                     });

        nodeVirtualThreads.put(hostname, virtualThread);
    }

    private void processEventWithRetry(EventWrapper eventWrapper, MemberInfo member) {
        EntityEvent<?> event = eventWrapper.event;
        boolean success = false;
        int attempts = 0;
        final String nodeId = member.hostname();

        while (!success && !isShutdown.get() && !Thread.currentThread().isInterrupted()) {
            attempts++;

            try {
                log.debug("Processing event {} for node {} (attempt {})", event.resourceName(), nodeId, attempts);

                ClusterMessage message = ClusterMessage.of(event);
                CompletableFuture<ResponseMessage> future = messageExchange.request(
                    "resource-events",
                    "process-event",
                    message
                ).orTimeout(eventProcessorConfig.getClusterMemberTimeoutMs(), TimeUnit.MILLISECONDS);

                ResponseMessage response = future.get();
                if (response.getException() != null) {
                    throw response.getException();
                }

                success = true;
                log.debug(
                    "Successfully processed event {} for node {} on attempt {}",
                    event.resourceName(),
                    member.hostname(),
                    attempts
                );
            } catch (Exception e) {
                long backoffMs = Math.min(
                    eventProcessorConfig.getRetryDelayMs() * (long)Math.pow(
                        2,
                        Math.min(eventProcessorConfig.getMaxBackoffAttempts(), attempts - 1)
                    ),
                    eventProcessorConfig.getMaxBackoffMs()
                );

                log.warn(
                    "Failed to process event {} for node {} (attempt {}), retrying in {} ms: {}",
                    event.resourceName(),
                    nodeId,
                    attempts,
                    backoffMs,
                    e.getMessage()
                );

                try {
                    CountDownLatch latch = new CountDownLatch(1);
                    boolean completed = latch.await(backoffMs, TimeUnit.MILLISECONDS);
                    if (!completed) {
                        log.debug(
                            "Retry delay of {} ms elapsed for event {}, continuing retry",
                            backoffMs,
                            event.resourceName()
                        );
                    }

                    if (Thread.currentThread().isInterrupted()) {
                        handleInterruption(event.resourceName(), nodeId);
                    }
                } catch (InterruptedException ie) {
                    handleInterruption(event.resourceName(), nodeId);
                }
            }
        }

        if (!success && !isShutdown.get() && !Thread.currentThread().isInterrupted()) {
            log.warn("Event {} processing for node {} exited retry loop without success", event.resourceName(), nodeId);
        }
    }

    private void handleInterruption(String resourceName, String nodeId) {
        log.info("Retry interrupted for event {} on node {}", resourceName, nodeId);
        Thread.currentThread().interrupt();
    }

    private void startCommitterThread() {
        committerThread = Thread.ofPlatform()
                                .name(eventProcessorConfig.getEventCommitterThreadName())
                                .daemon(true)
                                .priority(Thread.NORM_PRIORITY)
                                .start(() -> {
                                    log.info("Started event committer thread");

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

                                    handleRemainingEventsOnShutdown();
                                });
    }

    private void processNextCompletableEvent() throws InterruptedException {
        EventWrapper eventWrapper = inFlightEvents.take();

        synchronized (eventWrapper) {
            while (!isEventReady(eventWrapper) && !isShutdown.get()) {
                eventWrapper.wait(eventProcessorConfig.getEventReadyCheckMs());
            }
        }

        processEventIfAvailable(eventWrapper);
    }

    private boolean isEventReady(EventWrapper eventWrapper) {
        return eventWrapper.isCompleteForAllNodes() || eventWrapper.hasNodesChanged();
    }

    private void processEventIfAvailable(EventWrapper eventWrapper) {
        EventWrapper processed = inFlightEvents.poll();
        if (processed == null || processed != eventWrapper) {
            log.warn("Expected to process event wrapper but found a different one or none");
            return;
        }

        if (eventWrapper.hasNodesChanged()) {
            handleNodesChanged(eventWrapper);
        } else if (eventWrapper.isCompleteForAllNodes()) {
            handleEventCompletion(eventWrapper);
        } else {
            handleShutdownInterruption(eventWrapper);
        }
    }

    private void handleNodesChanged(EventWrapper eventWrapper) {
        log.info("Nodes changed during processing of event {}, reprocessing", eventWrapper.event.resourceName());
        enqueueEventToAllNodes(eventWrapper.event, eventWrapper.completeFuture);
    }

    private void handleEventCompletion(EventWrapper eventWrapper) {
        log.debug("Event {} processed by all nodes, completing", eventWrapper.event.resourceName());
        eventWrapper.completeFuture.complete(null);
    }

    private void handleShutdownInterruption(EventWrapper eventWrapper) {
        eventWrapper.completeFuture.completeExceptionally(
            new EventProcessingException("Processing interrupted due to shutdown")
        );
    }

    private void handleRemainingEventsOnShutdown() {
        log.info("Event committer thread is shutting down");

        EventWrapper remaining;
        while ((remaining = inFlightEvents.poll()) != null) {
            remaining.completeFuture.completeExceptionally(
                new EventProcessingException("EventProcessor is shutting down")
            );
        }
    }

    private void setupMembershipListener(VaradhiClusterManager clusterManager) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(MemberInfo memberInfo) {
                String hostname = memberInfo.hostname();
                log.info("New member joined: {}", hostname);

                BlockingQueue<EventWrapper> queue = new LinkedBlockingQueue<>();
                nodeEventQueues.put(hostname, queue);

                startNodeVirtualThread(memberInfo, queue);

                inFlightEvents.forEach(EventWrapper::markNodesChanged);

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

                inFlightEvents.forEach(EventWrapper::markNodesChanged);

                return CompletableFuture.completedFuture(null);
            }
        });
    }

    @Override
    public <T> CompletableFuture<Void> process(EntityEvent<T> event) {
        if (isShutdown.get()) {
            return CompletableFuture.failedFuture(new EventProcessingException("EventProcessor is shutdown"));
        }

        CompletableFuture<Void> completeFuture = new CompletableFuture<>();

        enqueueEventToAllNodes(event, completeFuture);

        return completeFuture;
    }

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

            boolean nodesChanged = !currentNodes.equals(Set.copyOf(nodeEventQueues.keySet()));
            if (nodesChanged) {
                eventWrapper.markNodesChanged();
                return;
            }

            for (String hostname : currentNodes) {
                BlockingQueue<EventWrapper> nodeQueue = nodeEventQueues.get(hostname);
                if (nodeQueue != null) {
                    nodeQueue.add(eventWrapper);
                } else {
                    eventWrapper.markNodesChanged();
                }
            }

            log.debug("Enqueued event {} to {} nodes", event.resourceName(), currentNodes.size());
        }
    }

    public void close() {
        if (isShutdown.compareAndSet(false, true)) {
            log.info("Shutting down EventProcessor");

            try {
                if (committerThread != null) {
                    committerThread.join(eventProcessorConfig.getThreadJoinTimeoutPrimaryMs());
                }

                if (committerThread != null && committerThread.isAlive()) {
                    committerThread.interrupt();
                }

                for (Map.Entry<String, Thread> entry : nodeVirtualThreads.entrySet()) {
                    String hostname = entry.getKey();
                    Thread thread = entry.getValue();
                    log.debug("Interrupting virtual thread for node: {}", hostname);
                    thread.interrupt();
                    thread.join(eventProcessorConfig.getThreadJoinTimeoutWorkerMs());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for threads to shut down", e);
                Thread.currentThread().interrupt();
            }

            nodeEventQueues.clear();
            nodeVirtualThreads.clear();

            log.info("EventProcessor shutdown complete");
        }
    }
}

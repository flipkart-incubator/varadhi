package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.common.exceptions.EventProcessingException;
import com.flipkart.varadhi.controller.EntityEventProcessor;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.EntityEvent;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class EventProcessor implements EntityEventProcessor {

    private final Vertx vertx;
    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final AtomicBoolean isShutdown;
    private final Semaphore concurrencyLimiter;

    private final AtomicBoolean membershipChanged = new AtomicBoolean(false);
    private final Map<String, MemberInfo> memberCache = new ConcurrentHashMap<>();

    public static Future<EventProcessor> create(
        Vertx vertx,
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
        EventProcessorConfig eventProcessorConfig
    ) {
        Promise<EventProcessor> promise = Promise.promise();
        EventProcessor processor = new EventProcessor(vertx, messageExchange, eventProcessorConfig);

        clusterManager.getAllMembers().compose(initialMembers -> {
            if (!initialMembers.isEmpty()) {
                processor.initializeMemberCache(initialMembers);
                processor.setupMembershipListener(clusterManager);
                return Future.succeededFuture(processor.closeOnError());
            }
            return Future.failedFuture("No members found in cluster");
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

    private EventProcessor closeOnError() {
        isShutdown.set(false);
        return this;
    }

    private EventProcessor(Vertx vertx, MessageExchange messageExchange, EventProcessorConfig eventProcessorConfig) {
        this.vertx = vertx;
        this.messageExchange = messageExchange;
        this.eventProcessorConfig = eventProcessorConfig != null ?
            eventProcessorConfig :
            EventProcessorConfig.getDefault();
        this.isShutdown = new AtomicBoolean(true);
        this.concurrencyLimiter = new Semaphore(this.eventProcessorConfig.getMaxConcurrentProcessing());
    }

    private void initializeMemberCache(List<MemberInfo> members) {
        members.forEach(member -> {
            memberCache.put(member.hostname(), member);
            log.info("Added member to cache: {}", member.hostname());
        });
        log.info("Initialized member cache with {} members", memberCache.size());
    }

    private void setupMembershipListener(VaradhiClusterManager clusterManager) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(MemberInfo memberInfo) {
                memberCache.put(memberInfo.hostname(), memberInfo);
                membershipChanged.set(true);
                log.info("New member added to cluster: {}", memberInfo.hostname());
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String id) {
                memberCache.remove(id);
                membershipChanged.set(true);
                log.info("Member removed from cluster: {}", id);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    @Override
    public CompletableFuture<Void> process(EntityEvent event) {
        if (isShutdown.get()) {
            return CompletableFuture.failedFuture(new EventProcessingException("EventProcessor is shutdown"));
        }

        Promise<Void> promise = Promise.promise();

        acquirePermit().compose(ignored -> processWithTimeout(event)).onComplete(ar -> {
            concurrencyLimiter.release();
            if (ar.succeeded()) {
                promise.complete();
            } else {
                handleProcessingFailure(event, ar.cause());
                promise.fail(ar.cause());
            }
        });

        return promise.future().toCompletionStage().toCompletableFuture();
    }

    private Future<Void> processWithTimeout(EntityEvent event) {
        if (memberCache.isEmpty()) {
            return Future.failedFuture(new EventProcessingException("No cluster members available for processing"));
        }

        List<MemberInfo> members = new ArrayList<>(memberCache.values());
        Promise<Void> promise = Promise.promise();

        long timerId = vertx.setTimer(eventProcessorConfig.getProcessingTimeout().toMillis(), id -> {
            if (!promise.future().isComplete()) {
                promise.fail(
                    new TimeoutException(
                        "Processing timed out after " + eventProcessorConfig.getProcessingTimeout().toMillis() + "ms"
                    )
                );
            }
        });

        processEventForAllClusterMembers(event, members).onComplete(ar -> {
            vertx.cancelTimer(timerId);
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    private Future<Void> processEventForAllClusterMembers(EntityEvent event, List<MemberInfo> members) {
        if (members.isEmpty()) {
            return Future.failedFuture(new EventProcessingException("No cluster members available"));
        }

        Map<String, ClusterMemberEventState> clusterMemberStates = new ConcurrentHashMap<>();
        members.forEach(
            member -> clusterMemberStates.put(
                member.hostname(),
                new ClusterMemberEventState(member, eventProcessorConfig.getMaxRetries())
            )
        );

        return processClusterMembersWithRetry(event, clusterMemberStates);
    }

    private Future<Void> processClusterMembersWithRetry(
        EntityEvent event,
        Map<String, ClusterMemberEventState> clusterMemberStates
    ) {
        Promise<Void> promise = Promise.promise();

        processNextBatch(event, clusterMemberStates, promise, 0);

        return promise.future();
    }

    private void processNextBatch(
        EntityEvent event,
        Map<String, ClusterMemberEventState> clusterMemberStates,
        Promise<Void> result,
        int attempt
    ) {
        if (isShutdown.get()) {
            result.fail(new EventProcessingException("EventProcessor shutdown during processing"));
            return;
        }

        if (hasMembershipChanged()) {
            handleMembershipChange(event, clusterMemberStates, result);
            return;
        }

        List<ClusterMemberEventState> pendingClusterMembers = clusterMemberStates.values()
                                                                                 .stream()
                                                                                 .filter(
                                                                                     state -> !state.isComplete()
                                                                                              && state.hasRetriesLeft()
                                                                                 )
                                                                                 .toList();

        if (pendingClusterMembers.isEmpty()) {
            handleCompletion(clusterMemberStates, result);
            return;
        }

        if (attempt >= eventProcessorConfig.getMaxRetries()) {
            handleMaxRetriesExceeded(clusterMemberStates, result);
            return;
        }

        processBatch(event, pendingClusterMembers, clusterMemberStates, result, attempt);
    }

    private boolean hasMembershipChanged() {
        return membershipChanged.get();
    }

    private void handleMembershipChange(
        EntityEvent event,
        Map<String, ClusterMemberEventState> currentStates,
        Promise<Void> result
    ) {
        Set<String> newMemberSet = memberCache.keySet();

        Set<String> removedMembers = new HashSet<>(currentStates.keySet());
        removedMembers.removeAll(newMemberSet);

        Set<String> addedMembers = new HashSet<>(newMemberSet);
        addedMembers.removeAll(currentStates.keySet());

        for (String removedMember : removedMembers) {
            ClusterMemberEventState state = currentStates.remove(removedMember);
            if (state != null && !state.isComplete()) {
                log.warn("Member {} removed during event processing", removedMember);
            }
        }

        for (String addedMember : addedMembers) {
            MemberInfo newMember = memberCache.get(addedMember);
            if (newMember != null) {
                currentStates.put(
                    addedMember,
                    new ClusterMemberEventState(newMember, eventProcessorConfig.getMaxRetries())
                );
                log.info("New member {} added during event processing", addedMember);
            }
        }

        if (!removedMembers.isEmpty() || !addedMembers.isEmpty()) {
            log.info(
                "Cluster membership changed during event processing. Removed: {}, Added: {}",
                removedMembers,
                addedMembers
            );
        }

        membershipChanged.set(false);

        if (currentStates.isEmpty()) {
            result.fail(new EventProcessingException("All cluster members were removed during processing"));
        } else {
            processNextBatch(event, currentStates, result, 0);
        }
    }

    private void processBatch(
        EntityEvent event,
        List<ClusterMemberEventState> pendingClusterMembers,
        Map<String, ClusterMemberEventState> clusterMemberStates,
        Promise<Void> result,
        int attempt
    ) {
        List<Future<Void>> clusterMemberFutures = pendingClusterMembers.stream()
                                                                       .map(
                                                                           clusterMemberState -> processClusterMember(
                                                                               event,
                                                                               clusterMemberState
                                                                           )
                                                                       )
                                                                       .toList();

        CompositeFuture.join(new ArrayList<>(clusterMemberFutures)).onComplete(ar -> {
            if (ar.failed()) {
                log.warn("Batch processing encountered errors", ar.cause());
            }
            vertx.setTimer(
                eventProcessorConfig.getRetryDelayMs(),
                id -> processNextBatch(event, clusterMemberStates, result, attempt + 1)
            );
        });
    }

    private void handleMaxRetriesExceeded(
        Map<String, ClusterMemberEventState> clusterMemberStates,
        Promise<Void> result
    ) {
        List<String> failedClusterMembers = clusterMemberStates.values()
                                                               .stream()
                                                               .filter(state -> !state.isComplete())
                                                               .map(state -> state.getMember().hostname())
                                                               .toList();

        String errorMessage = String.format(
            "Event processing failed after %d retries for clusterMembers: %s",
            eventProcessorConfig.getMaxRetries(),
            failedClusterMembers
        );

        log.error(errorMessage);

        result.fail(new EventProcessingException(errorMessage));
    }

    private Future<Void> processClusterMember(EntityEvent event, ClusterMemberEventState clusterMemberState) {
        String clusterMemberId = clusterMemberState.getMember().hostname();

        if (hasMembershipChanged()) {
            return Future.failedFuture(
                new EventProcessingException("Cluster membership changed before processing member: " + clusterMemberId)
            );
        }

        return vertx.executeBlocking(promise -> {
            try {
                ClusterMessage message = ClusterMessage.of(event);
                messageExchange.request("cache-events", "processEvent", message)
                               .orTimeout(eventProcessorConfig.getClusterMemberTimeoutMs(), TimeUnit.MILLISECONDS)
                               .thenAccept(response -> {
                                   if (response.getException() != null) {
                                       handleClusterMemberFailure(clusterMemberState, response.getException());
                                       promise.fail(
                                           new EventProcessingException(
                                               "ClusterMember processing failed: " + response.getException()
                                                                                             .getMessage()
                                           )
                                       );
                                   } else {
                                       clusterMemberState.markComplete();
                                       promise.complete();
                                   }
                               })
                               .exceptionally(throwable -> {
                                   promise.fail(throwable);
                                   return null;
                               });
            } catch (Exception e) {
                promise.fail(e);
            }
        });
    }

    private void handleClusterMemberFailure(ClusterMemberEventState clusterMemberState, Throwable throwable) {
        String clusterMemberId = clusterMemberState.getMember().hostname();
        clusterMemberState.incrementRetries();

        if (!clusterMemberState.hasRetriesLeft()) {
            log.error(
                "ClusterMember {} failed permanently after {} retries. Error: {}",
                clusterMemberId,
                eventProcessorConfig.getMaxRetries(),
                throwable.getMessage()
            );
        } else {
            log.warn(
                "ClusterMember {} failed temporarily (attempt {}). Error: {}",
                clusterMemberId,
                clusterMemberState.getRetryCount(),
                throwable.getMessage()
            );
        }
    }

    private void handleCompletion(Map<String, ClusterMemberEventState> clusterMemberStates, Promise<Void> result) {
        boolean allComplete = clusterMemberStates.values().stream().allMatch(ClusterMemberEventState::isComplete);

        if (allComplete) {
            result.complete(null);
        } else {
            List<String> failedClusterMembers = clusterMemberStates.values()
                                                                   .stream()
                                                                   .filter(state -> !state.isComplete())
                                                                   .map(state -> state.getMember().hostname())
                                                                   .toList();
            result.fail(
                new EventProcessingException("Event processing failed for clusterMembers: " + failedClusterMembers)
            );
        }
    }

    private void handleProcessingFailure(EntityEvent event, Throwable throwable) {
        log.error("Failed to process event: {}/{}", event.resourceType(), event.resourceName(), throwable);
    }

    public void close() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        try {
            if (!concurrencyLimiter.tryAcquire(
                eventProcessorConfig.getMaxConcurrentProcessing(),
                eventProcessorConfig.getProcessingTimeout().toMillis(),
                TimeUnit.MILLISECONDS
            )) {
                log.warn("Shutdown timed out waiting for tasks to complete");
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown interrupted while waiting for tasks to complete", e);
            Thread.currentThread().interrupt();
        } finally {
            concurrencyLimiter.release(eventProcessorConfig.getMaxConcurrentProcessing());
        }
    }

    private Future<Void> acquirePermit() {
        return vertx.executeBlocking(promise -> {
            try {
                if (!concurrencyLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
                    promise.fail(
                        new EventProcessingException("Failed to acquire processing permit - system overloaded")
                    );
                } else {
                    promise.complete();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                promise.fail(new EventProcessingException("Interrupted while acquiring permit", e));
            }
        });
    }
}

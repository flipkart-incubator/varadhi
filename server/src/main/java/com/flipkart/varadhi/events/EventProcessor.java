package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.controller.EntityEventProcessor;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.EntityEvent;
import com.flipkart.varadhi.common.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Slf4j
public class EventProcessor implements EntityEventProcessor, AutoCloseable {

    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isShutdown;
    private final Semaphore concurrencyLimiter;

    private final AtomicBoolean membershipChanged = new AtomicBoolean(false);
    private final Map<String, MemberInfo> memberCache = new ConcurrentHashMap<>();

    public EventProcessor(
            MessageExchange messageExchange,
            VaradhiClusterManager clusterManager,
            EventProcessorConfig eventProcessorConfig
    ) {
        this.messageExchange = messageExchange;
        this.eventProcessorConfig = eventProcessorConfig != null ?
                eventProcessorConfig :
                EventProcessorConfig.getDefault();
        this.scheduler = createScheduler();
        this.isShutdown = new AtomicBoolean(false);
        this.concurrencyLimiter = new Semaphore(this.eventProcessorConfig.getMaxConcurrentProcessing());

        try {
            List<MemberInfo> initialMembers = clusterManager.getAllMembers()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);

            initialMembers.forEach(member -> memberCache.put(member.hostname(), member));
            log.info("Initialized member cache with {} members", memberCache.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventProcessingException("Interrupted while initializing cluster member cache", e);
        } catch (Exception e) {
            throw new EventProcessingException("Failed to initialize cluster member cache", e);
        }

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

    private ScheduledExecutorService createScheduler() {
        return Executors.newScheduledThreadPool(
                Runtime.getRuntime().availableProcessors(),
                Thread.ofVirtual()
                        .name("event-processor-", 0)
                        .uncaughtExceptionHandler(this::handleUncaughtException)
                        .factory()
        );
    }

    private void handleUncaughtException(Thread thread, Throwable throwable) {
        log.error("Uncaught exception in thread {}", thread.getName(), throwable);
    }

    @Override
    public CompletableFuture<Void> process(EntityEvent event) {
        if (isShutdown.get()) {
            return CompletableFuture.failedFuture(new EventProcessingException("EventProcessor is shutdown"));
        }

        return acquirePermit().thenCompose(ignored -> processWithTimeout(event)).whenComplete((ignored, throwable) -> {
            concurrencyLimiter.release();
            if (throwable != null) {
                handleProcessingFailure(event, throwable);
            }
        });
    }

    private CompletableFuture<Void> processWithTimeout(EntityEvent event) {
        if (memberCache.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new EventProcessingException("No cluster members available for processing")
            );
        }

        List<MemberInfo> members = new ArrayList<>(memberCache.values());

        return CompletableFuture.supplyAsync(
                        () -> processEventForAllClusterMembers(event, members),
                        scheduler
                )
                .thenCompose(Function.identity())
                .orTimeout(eventProcessorConfig.getProcessingTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> processEventForAllClusterMembers(EntityEvent event, List<MemberInfo> members) {
        if (members.isEmpty()) {
            return CompletableFuture.failedFuture(new EventProcessingException("No cluster members available"));
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

    private CompletableFuture<Void> processClusterMembersWithRetry(
            EntityEvent event,
            Map<String, ClusterMemberEventState> clusterMemberStates
    ) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        processNextBatch(event, clusterMemberStates, result, 0);

        return result;
    }

    private void processNextBatch(
            EntityEvent event,
            Map<String, ClusterMemberEventState> clusterMemberStates,
            CompletableFuture<Void> result,
            int attempt
    ) {
        if (isShutdown.get()) {
            result.completeExceptionally(new EventProcessingException("EventProcessor shutdown during processing"));
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
            CompletableFuture<Void> result
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
            log.info("Cluster membership changed during event processing. Removed: {}, Added: {}",
                    removedMembers, addedMembers);
        }

        membershipChanged.set(false);

        if (currentStates.isEmpty()) {
            result.completeExceptionally(
                    new EventProcessingException("All cluster members were removed during processing")
            );
        } else {
            processNextBatch(event, currentStates, result, 0);
        }
    }

    private void processBatch(
            EntityEvent event,
            List<ClusterMemberEventState> pendingClusterMembers,
            Map<String, ClusterMemberEventState> clusterMemberStates,
            CompletableFuture<Void> result,
            int attempt
    ) {
        List<CompletableFuture<Void>> clusterMemberFutures = pendingClusterMembers.stream()
                .map(
                        clusterMemberState -> processClusterMember(
                                event,
                                clusterMemberState
                        )
                )
                .toList();

        CompletableFuture.allOf(clusterMemberFutures.toArray(new CompletableFuture[0])).whenComplete((v, throwable) -> {
            if (throwable != null) {
                log.warn("Batch processing encountered errors", throwable);
            }
            scheduler.schedule(
                    () -> processNextBatch(event, clusterMemberStates, result, attempt + 1),
                    eventProcessorConfig.getRetryDelayMs(),
                    TimeUnit.MILLISECONDS
            );
        });
    }

    private void handleMaxRetriesExceeded(
            Map<String, ClusterMemberEventState> clusterMemberStates,
            CompletableFuture<Void> result
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

        result.completeExceptionally(new EventProcessingException(errorMessage));
    }

    private CompletableFuture<Void> processClusterMember(
            EntityEvent event,
            ClusterMemberEventState clusterMemberState
    ) {
        String clusterMemberId = clusterMemberState.getMember().hostname();

        if (hasMembershipChanged()) {
            return CompletableFuture.failedFuture(
                    new EventProcessingException("Cluster membership changed before processing member: " + clusterMemberId)
            );
        }

        return CompletableFuture.supplyAsync(() -> {
            ClusterMessage message = ClusterMessage.of(event);
            return messageExchange.request("cache-events", "processEvent", message)
                    .orTimeout(
                            eventProcessorConfig.getClusterMemberTimeoutMs(),
                            TimeUnit.MILLISECONDS
                    )
                    .thenAccept(response -> {
                        if (response.getException() != null) {
                            handleClusterMemberFailure(clusterMemberState, response.getException());
                            throw new EventProcessingException(
                                    "ClusterMember processing failed: " + response.getException()
                                            .getMessage()
                            );
                        }
                        clusterMemberState.markComplete();
                    });
        }, scheduler).thenCompose(Function.identity());
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

    private void handleCompletion(
            Map<String, ClusterMemberEventState> clusterMemberStates,
            CompletableFuture<Void> result
    ) {
        boolean allComplete = clusterMemberStates.values()
                .stream()
                .allMatch(ClusterMemberEventState::isComplete);

        if (allComplete) {
            result.complete(null);
        } else {
            List<String> failedClusterMembers = clusterMemberStates.values()
                    .stream()
                    .filter(state -> !state.isComplete())
                    .map(state -> state.getMember().hostname())
                    .toList();
            result.completeExceptionally(
                    new EventProcessingException("Event processing failed for clusterMembers: " + failedClusterMembers)
            );
        }
    }

    private void handleProcessingFailure(EntityEvent event, Throwable throwable) {
        log.error("Failed to process event: {}/{}", event.resourceType(), event.resourceName(), throwable);
    }

    @Override
    public void close() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        try {
            log.info("Initiating EventProcessor shutdown...");

            scheduler.shutdown();
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }

            log.info("EventProcessor shutdown completed");
        } catch (Exception e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            throw new EventProcessingException("Failed to shutdown EventProcessor", e);
        }
    }

    private CompletableFuture<Void> acquirePermit() {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!concurrencyLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
                    throw new EventProcessingException("Failed to acquire processing permit - system overloaded");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(new EventProcessingException("Interrupted while acquiring permit", e));
            }
        }, scheduler);
    }
}

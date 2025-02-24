package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.ResourceEvent;
import com.flipkart.varadhi.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Processes resource events across a distributed cluster with retry and timeout capabilities.
 * This class handles the distribution and coordination of events to all cluster members,
 * ensuring reliable delivery and processing with configurable retry policies.
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Concurrent event processing with configurable limits</li>
 *   <li>Automatic retry mechanism for failed cluster members</li>
 *   <li>Timeout handling for both overall processing and individual cluster members</li>
 *   <li>Virtual thread utilization for scalable processing</li>
 *   <li>Graceful shutdown handling</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * The processor can be configured using {@link EventProcessorConfig} with settings for:
 * <ul>
 *   <li>Maximum retry attempts</li>
 *   <li>Retry delay intervals</li>
 *   <li>Cluster member timeouts</li>
 *   <li>Concurrent processing limits</li>
 *   <li>Overall processing timeout</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * Config config = Config.builder()
 *     .maxRetries(3)
 *     .retryDelayMs(1000)
 *     .maxConcurrentProcessing(100)
 *     .build();
 *
 * EventProcessor processor = new EventProcessor(messageExchange, clusterManager, config);
 * processor.process(resourceEvent)
 *     .thenAccept(result -> log.info("Event processed successfully"))
 *     .exceptionally(throwable -> {
 *         log.error("Event processing failed", throwable);
 *         return null;
 *     });
 * }</pre>
 *
 * <h2>Error Handling</h2>
 * The processor handles several types of failures:
 * <ul>
 *   <li>Individual cluster member failures with retries</li>
 *   <li>Timeout scenarios for both overall processing and individual members</li>
 *   <li>Concurrent processing limits with backpressure</li>
 *   <li>Shutdown state validation</li>
 * </ul>
 *
 * <h2>Implementation Notes</h2>
 * <ul>
 *   <li>Uses virtual threads for efficient resource utilization</li>
 *   <li>Implements backpressure using semaphores</li>
 *   <li>Maintains thread safety using concurrent data structures</li>
 *   <li>Supports graceful shutdown with resource cleanup</li>
 * </ul>
 *
 * @see MessageExchange For cluster communication
 * @see VaradhiClusterManager For cluster management
 * @see ResourceEvent For event data structure
 */
@Slf4j
public class EventProcessor {
    static final String EVENT_ADDRESS = "varadhi.events";

    private final EventProcessorConfig eventProcessorConfig;
    private final MessageExchange messageExchange;
    private final VaradhiClusterManager clusterManager;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isShutdown;
    private final Semaphore concurrencyLimiter;

    /**
     * Creates a new EventProcessor instance with the specified configuration.
     *
     * @param messageExchange The exchange for cluster communication
     * @param clusterManager  The manager for cluster operations
     * @param eventProcessorConfig          The processor configuration (uses default if null)
     * @throws NullPointerException if messageExchange or clusterManager is null
     */
    public EventProcessor(
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
        EventProcessorConfig eventProcessorConfig
    ) {
        this.messageExchange = messageExchange;
        this.clusterManager = clusterManager;
        this.eventProcessorConfig = eventProcessorConfig != null ?
            eventProcessorConfig :
            EventProcessorConfig.getDefault();
        this.scheduler = createScheduler();
        this.isShutdown = new AtomicBoolean(false);
        this.concurrencyLimiter = new Semaphore(this.eventProcessorConfig.getMaxConcurrentProcessing());
    }

    /**
     * Creates a virtual thread pool scheduler for processing events.
     * Uses virtual threads for efficient resource utilization and scales
     * based on available processors.
     *
     * @return A new ScheduledExecutorService configured with virtual threads
     */
    private ScheduledExecutorService createScheduler() {
        return Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            Thread.ofVirtual()
                  .name("event-processor-", 0)
                  .uncaughtExceptionHandler(this::handleUncaughtException)
                  .factory()
        );
    }

    /**
     * Handles uncaught exceptions from virtual threads.
     *
     * @param thread    The thread where the exception occurred
     * @param throwable The uncaught exception
     */
    private void handleUncaughtException(Thread thread, Throwable throwable) {
        log.error("Uncaught exception in thread {}", thread.getName(), throwable);
    }

    /**
     * Processes a resource event across all cluster members with retry capabilities.
     * Implements backpressure using a semaphore to limit concurrent processing.
     *
     * @param event The event to process
     * @return A future that completes when the event is processed by all members
     * @throws EventProcessingException if the processor is shutdown or processing fails
     */
    CompletableFuture<Void> process(ResourceEvent event) {
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

    /**
     * Processes an event with an overall timeout.
     * The timeout is configured via {@link EventProcessorConfig}.
     *
     * @param event The event to process
     * @return A future that completes when processing is done or times out
     */
    private CompletableFuture<Void> processWithTimeout(ResourceEvent event) {
        return CompletableFuture.supplyAsync(
            () -> clusterManager.getAllMembers()
                                .toCompletionStage()
                                .toCompletableFuture()
                                .thenCompose(members -> processEventForAllClusterMembers(event, members))
                                .orTimeout(
                                    eventProcessorConfig.getProcessingTimeout().toMillis(),
                                    TimeUnit.MILLISECONDS
                                ),
            scheduler
        ).thenCompose(Function.identity());
    }

    /**
     * Processes an event across all cluster members.
     * Creates state tracking for each member and initiates the retry process.
     *
     * @param event   The event to process
     * @param members The list of cluster members
     * @return A future that completes when all members have processed the event
     * @throws EventProcessingException if no cluster members are available
     */
    private CompletableFuture<Void> processEventForAllClusterMembers(ResourceEvent event, List<MemberInfo> members) {
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

        return processClusterMembersWithRetry(event, clusterMemberStates).orTimeout(
            eventProcessorConfig.getClusterMemberTimeoutMs() * 2,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Processes cluster members with retry capability.
     * Handles batch processing and scheduling of retries.
     *
     * @param event               The event being processed
     * @param clusterMemberStates Map of member states for tracking progress
     * @return A future that completes when all members have processed or max retries reached
     */
    private CompletableFuture<Void> processClusterMembersWithRetry(
        ResourceEvent event,
        Map<String, ClusterMemberEventState> clusterMemberStates
    ) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        processNextBatch(event, clusterMemberStates, result, 0);

        return result;
    }

    /**
     * Processes the next batch of pending cluster members.
     * Handles completion, retry limits, and schedules next batch processing.
     *
     * @param event               The event being processed
     * @param clusterMemberStates Map of member states
     * @param result              The future to complete when processing is done
     * @param attempt             The current retry attempt number
     */
    private void processNextBatch(
        ResourceEvent event,
        Map<String, ClusterMemberEventState> clusterMemberStates,
        CompletableFuture<Void> result,
        int attempt
    ) {
        if (isShutdown.get()) {
            result.completeExceptionally(new EventProcessingException("EventProcessor shutdown during processing"));
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

    /**
     * Processes a batch of cluster members in parallel.
     * Schedules the next batch after completion or failure.
     *
     * @param event                 The event being processed
     * @param pendingClusterMembers List of members to process
     * @param clusterMemberStates   Complete map of member states
     * @param result                The future to complete when all processing is done
     * @param attempt               The current retry attempt number
     */
    private void processBatch(
        ResourceEvent event,
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

    /**
     * Handles the case when max retries are exceeded.
     * Completes the result future exceptionally with details of failed members.
     *
     * @param clusterMemberStates Map of member states
     * @param result              The future to complete exceptionally
     */
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

    /**
     * Processes a single cluster member with timeout.
     * Sends the event and handles the response or failure.
     *
     * @param event              The event to process
     * @param clusterMemberState The state of the member being processed
     * @return A future that completes when the member processes the event
     */
    private CompletableFuture<Void> processClusterMember(
        ResourceEvent event,
        ClusterMemberEventState clusterMemberState
    ) {
        String clusterMemberId = clusterMemberState.getMember().hostname();

        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterMessage message = ClusterMessage.of(event);
                return messageExchange.request(clusterMemberId, "processEvent", message)
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
            } catch (Exception e) {
                handleClusterMemberFailure(clusterMemberState, e);
                throw e;
            }
        }, scheduler).thenCompose(Function.identity());
    }

    /**
     * Handles failure of a cluster member during processing.
     * Updates retry count and logs appropriate messages.
     *
     * @param clusterMemberState The state of the failed member
     * @param throwable          The error that occurred
     */
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

    /**
     * Handles the completion of event processing across all cluster members.
     * If any members failed to process the event, completes the result future exceptionally
     * with details of the failed members. Otherwise, completes the future successfully.
     *
     * @param clusterMemberStates Map of member states tracking processing status
     * @param result              The future to complete based on overall processing status
     */
    private void handleCompletion(
        Map<String, ClusterMemberEventState> clusterMemberStates,
        CompletableFuture<Void> result
    ) {
        List<String> failedClusterMembers = clusterMemberStates.values()
                                                               .stream()
                                                               .filter(state -> !state.isComplete())
                                                               .map(state -> state.getMember().hostname())
                                                               .toList();

        if (failedClusterMembers.isEmpty()) {
            result.complete(null);
        } else {
            result.completeExceptionally(
                new EventProcessingException("Event processing failed for clusterMembers: " + failedClusterMembers)
            );
        }
    }

    /**
     * Handles and logs event processing failures.
     * Provides error logging with event details for monitoring and debugging.
     *
     * @param event     The event that failed to process
     * @param throwable The error that caused the processing failure
     */
    private void handleProcessingFailure(ResourceEvent event, Throwable throwable) {
        log.error("Failed to process event: {}", event.eventName(), throwable);
    }

    /**
     * Gracefully shuts down the event processor.
     * Waits for pending tasks to complete and releases resources.
     *
     * @throws EventProcessingException if shutdown fails
     */
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
            log.error("Error during EventProcessor shutdown", e);
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            throw new EventProcessingException("Failed to shutdown EventProcessor", e);
        }
    }

    /**
     * Acquires a permit from the semaphore for processing.
     * Implements backpressure by limiting concurrent processing.
     *
     * @return A future that completes when a permit is acquired
     * @throws EventProcessingException if permit acquisition fails or is interrupted
     */
    private CompletableFuture<Void> acquirePermit() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!concurrencyLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
                    throw new EventProcessingException("Failed to acquire processing permit - system overloaded");
                }
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(new EventProcessingException("Interrupted while acquiring permit", e));
            }
        }, scheduler);
    }
}

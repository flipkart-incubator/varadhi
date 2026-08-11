package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.common.utils.RetryUtils;
import com.flipkart.varadhi.common.utils.ThrowableUtils;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.controller.TransitionAckApi;
import com.flipkart.varadhi.core.cluster.failover.TransitionEventListener;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.produce.ProducerService;
import dev.failsafe.FailsafeExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.*;

/**
 * Minimal pod-side handler for topic-transition stage broadcasts (topic failover and
 * storage-topic migration alike). It reacts using only the self-contained
 * {@link TransitionEvent} and the pod's local {@code TopicCache}.
 *
 * <p>Participation ({@link TransitionParticipation#INVOLVED} vs
 * {@link TransitionParticipation#NOT_INVOLVED}) is decided at PREPARE and echoed on every
 * subsequent ack so the controller always knows pod involvement without an op-store lookup.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local TopicCache to
 * converge to the <em>exact</em> coordinated version before acking; all others ack immediately
 * on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll until the cache
 *       observes exactly version N, then if this pod is already producing the topic pre-warm the
 *       target producer ({@link TransitionParticipation#INVOLVED}); otherwise stay
 *       {@link TransitionParticipation#NOT_INVOLVED} (no producer created; fencing can plug in
 *       later) and ack. A stale/unreachable pod times out, and a warm failure acks failure — both
 *       let the controller abort before any change.</li>
 *   <li><b>SWITCH</b> ({@code topicVersionToAwait} = N+1) — convergence: same exact-version
 *       wait but for N+1, then ack.</li>
 *   <li>For version-gated stages, if the cache has already moved <em>past</em> the target, the
 *       pod acks failure, treating it as a concurrent modification so the controller can
 *       abort/retry.</li>
 *   <li><b>PENDING / COMPLETED / ABORTED</b> ({@code awaitVersion} = false) — no version to
 *       await; ack immediately so the controller knows the pod processed the stage.</li>
 * </ul>
 *
 * <p>Version wait uses exceptions as control flow ({@code <} retry, {@code =} success,
 * {@code >} fail). Acks are applied once at the end of the async chain ({@code thenAccept} /
 * {@code exceptionally}), not from every helper.
 *
 * <p>Threading: Failsafe version polls run on the <em>retry</em> scheduler; orchestration
 * (metrics, PREPARE decision, ack) hops to the dedicated <em>transition</em> executor.
 * Producer warm may complete on the producer path; {@code thenAcceptAsync}/{@code exceptionallyAsync}
 * switch back to the transition executor before ack.
 */
@Slf4j
public final class ProduceTransitionMsgHandler implements TransitionEventListener {

    /**
     * Probe signal: TopicCache is still behind the coordinated version. Failsafe retries until
     * the version catches up or attempts are exhausted ({@link java.util.concurrent.TimeoutException}).
     */
    static final class StaleVersionException extends RuntimeException {
        StaleVersionException(long current, long target) {
            super("topic version " + current + " < target " + target);
        }
    }

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final TransitionAckApi transitionAckApi;
    private final ProducerService producerService;
    private final TransitionMetrics metrics;
    private final FailsafeExecutor<TransitionEvent> versionWaitExecutor;
    private final Executor transitionExecutor;
    private final ConcurrentMap<String, TransitionParticipation> participationByOpId = new ConcurrentHashMap<>();

    public ProduceTransitionMsgHandler(
        String hostname,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        TransitionAckApi transitionAckApi,
        ProducerService producerService,
        PodTransitionConfig config,
        ScheduledExecutorService versionWaitScheduler,
        Executor transitionExecutor,
        TransitionMetrics metrics
    ) {
        this.hostname = hostname;
        this.topicCache = topicCache;
        this.transitionAckApi = transitionAckApi;
        this.producerService = producerService;
        this.metrics = metrics;
        this.transitionExecutor = transitionExecutor;
        this.versionWaitExecutor = RetryUtils.newPollingExecutor(
            versionWaitScheduler,
            config.versionWaitMaxAttempts(),
            config.podPollIntervalMs(),
            StaleVersionException.class
        );
    }

    @Override
    public void onTransition(TransitionEvent event) {
        metrics.stageReceived(event.transitionType(), event.stage());
        metrics.setTopicStage(event.topicFqn().toFqn(), event.stage());
        if (!event.awaitVersion()) {
            // Keep event-bus thread free; ack on transition executor.
            CompletableFuture.runAsync(() -> ackOk(event), transitionExecutor);
            return;
        }
        String topicFqn = event.topicFqn().toFqn();
        // probe → retry TP; metrics / PREPARE / ack → transition TP (warm may leave briefly then hop back).
        versionWaitExecutor.getAsync(() -> probeVersion(event))
                           .thenComposeAsync(this::onVersionReached, transitionExecutor)
                           .thenAccept(this::ackOk)
                           .exceptionally(t -> {
                               log.error("transition version wait failed for {} op={}", topicFqn, event.opId(), t);
                               ackFail(event, "transition version wait failed: " + ThrowableUtils.rootMessage(t));
                               return null;
                           });
    }

    /**
     * Exact-version gate: {@code <} → retry, {@code =} → return event, {@code >} → fail loud.
     */
    private TransitionEvent probeVersion(TransitionEvent event) {
        String topicFqn = event.topicFqn().toFqn();
        Optional<Resource.EntityResource<VaradhiTopic>> cached = topicCache.get(topicFqn);
        if (cached.isEmpty()) {
            throw new ResourceNotFoundException("Topic(%s) does not exist in topic cache.".formatted(topicFqn));
        }
        long version = cached.get().getVersion();
        long target = event.topicVersionToAwait();
        if (version < target) {
            throw new StaleVersionException(version, target);
        }
        if (version > target) {
            throw new VaradhiException(
                "topic version overshot target " + target + " (current " + version + "), concurrent modification"
            );
        }
        return event;
    }

    private CompletableFuture<TransitionEvent> onVersionReached(TransitionEvent event) {
        if (event.stage() != TransitionStage.PREPARE) {
            return CompletableFuture.completedFuture(event);
        }
        VaradhiTopic topic = topicCache.get(event.topicFqn().toFqn())
                                       .map(Resource.EntityResource::getEntity)
                                       .orElseThrow(
                                           () -> new ResourceNotFoundException(
                                               "Topic(%s) does not exist in topic cache.".formatted(
                                                   event.topicFqn().toFqn()
                                               )
                                           )
                                       );
        TransitionParticipation participation = producerService.hasActiveProducer(topic) ?
            TransitionParticipation.INVOLVED :
            TransitionParticipation.NOT_INVOLVED;
        recordParticipation(event, participation);
        if (participation == TransitionParticipation.NOT_INVOLVED) {
            log.debug(
                "Transition: pod not involved for {} op={} type={}; skipping participant work",
                event.topicFqn().toFqn(),
                event.opId(),
                event.transitionType()
            );
            return CompletableFuture.completedFuture(event);
        }
        return createTarget(event, topic);
    }

    /**
     * Type-specific PREPARE warm for an {@link TransitionParticipation#INVOLVED} pod.
     */
    private CompletableFuture<TransitionEvent> createTarget(TransitionEvent event, VaradhiTopic topic) {
        TransitionEvent.Target target = event.target();
        // TODO: Spotless removeUnusedImports can't parse nested record patterns.
        if (target instanceof TransitionEvent.Target.Region regionTarget) {
            return producerService.getProducerForRegion(topic, regionTarget.region()).thenApply(producer -> event);
        }
        if (target instanceof TransitionEvent.Target.StorageTopic storageTarget) {
            return producerService.loadProducer(event.topicFqn(), storageTarget.storageTopicId())
                                  .thenApply(producer -> event);
        }
        return CompletableFuture.failedFuture(new IllegalStateException("PREPARE target missing"));
    }

    private void recordParticipation(TransitionEvent event, TransitionParticipation participation) {
        participationByOpId.put(event.opId(), participation);
    }

    /**
     * Participation decided at PREPARE and sticky for the op. Late joiners (first event not PREPARE)
     * derive from whether this pod already has a producer for the topic's active produce target.
     */
    private TransitionParticipation resolveParticipation(TransitionEvent event) {
        return participationByOpId.computeIfAbsent(event.opId(), ignored -> deriveParticipation(event.topicFqn()));
    }

    private TransitionParticipation deriveParticipation(VaradhiTopicName topicFqn) {
        return topicCache.get(topicFqn.toFqn())
                         .map(Resource.EntityResource::getEntity)
                         .filter(producerService::hasActiveProducer)
                         .map(ignored -> TransitionParticipation.INVOLVED)
                         .orElse(TransitionParticipation.NOT_INVOLVED);
    }

    private void clearParticipationIfTerminal(TransitionEvent event) {
        if (event.stage() == TransitionStage.COMPLETED || event.stage() == TransitionStage.ABORTED) {
            participationByOpId.remove(event.opId());
            metrics.clearTopicStage(event.topicFqn().toFqn());
        }
    }

    private void ackOk(TransitionEvent event) {
        TransitionParticipation participation = resolveParticipation(event);
        metrics.stageAcked(event.transitionType(), event.stage(), event.topicFqn().toFqn(), true);
        sendAck(
            TransitionAck.success(
                event.opId(),
                event.topicFqn(),
                event.transitionType(),
                participation,
                hostname,
                event.stage()
            )
        );
        clearParticipationIfTerminal(event);
    }

    private void ackFail(TransitionEvent event, String errorMsg) {
        TransitionParticipation participation = resolveParticipation(event);
        metrics.stageAcked(event.transitionType(), event.stage(), event.topicFqn().toFqn(), false);
        sendAck(
            TransitionAck.failure(
                event.opId(),
                event.topicFqn(),
                event.transitionType(),
                participation,
                hostname,
                event.stage(),
                errorMsg
            )
        );
        clearParticipationIfTerminal(event);
    }

    private void sendAck(TransitionAck ack) {
        // Best-effort: if delivery fails, the controller stage barrier times out and re-pushes.
        transitionAckApi.ack(ack).whenComplete((ignored, t) -> {
            if (t != null) {
                metrics.ackSendFailed(ack.transitionType(), ack.stage());
                log.warn("Failed to deliver transition ack ack={}", ack, t);
            } else {
                metrics.ackSendSucceeded(ack.transitionType(), ack.stage());
            }
        });
    }
}

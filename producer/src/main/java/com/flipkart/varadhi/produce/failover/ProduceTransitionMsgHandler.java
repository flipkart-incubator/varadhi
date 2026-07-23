package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.common.utils.RetryUtils;
import com.flipkart.varadhi.common.utils.ThrowableUtils;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.controller.ControllerRouteApi;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.produce.ProducerService;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

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
 */
@Slf4j
public final class ProduceTransitionMsgHandler implements MsgHandler {

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final ControllerRouteApi controllerClient;
    private final ProducerService producerService;
    private final TransitionMetrics metrics;
    private final RetryUtils.ResultPollingExecutor<Optional<Long>> versionWaitExecutor;
    private final ConcurrentMap<String, TransitionParticipation> participationByOpId = new ConcurrentHashMap<>();

    public ProduceTransitionMsgHandler(
        String hostname,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        ControllerRouteApi controllerClient,
        ProducerService producerService,
        PodTransitionConfig config,
        ScheduledExecutorService scheduler,
        TransitionMetrics metrics
    ) {
        this.hostname = hostname;
        this.topicCache = topicCache;
        this.controllerClient = controllerClient;
        this.producerService = producerService;
        this.metrics = metrics;
        this.versionWaitExecutor = RetryUtils.newResultPollingExecutor(
            scheduler,
            config.versionWaitMaxAttempts(),
            config.podPollIntervalMs(),
            Optional::isEmpty
        );
    }

    @Override
    public void handle(ClusterMessage message) {
        TransitionEvent event = message.getData(TransitionEvent.class);
        metrics.stageReceived(event.transitionType(), event.stage());
        // Non-version-gated stages ack immediately on receipt.
        if (!event.awaitVersion()) {
            ackOk(event);
            return;
        }
        // handle() runs on the event-bus thread that delivered this publish. The version wait (and
        // the PREPARE pre-warm it triggers) runs on the transition scheduler via a reusable
        // versionWaitExecutor so the event bus is never stalled and the retry policy is not
        // rebuilt per event. Polling uses a fixed interval (not exponential backoff): cache
        // convergence within a deadline, not failure retry.
        String topicFqn = event.topicFqn().toFqn();
        metrics.versionWaitStarted();
        long targetVersion = event.topicVersionToAwait();
        RetryUtils.getAsync(versionWaitExecutor, () -> probeVersion(topicFqn, targetVersion))
                  .whenComplete((outcome, t) -> {
                      metrics.versionWaitFinished();
                      if (t != null) {
                          if (RetryUtils.isRetriesExceeded(t)) {
                              ackFail(event, versionWaitTimeoutMessage(targetVersion, topicFqn));
                              return;
                          }
                          log.error("transition version wait failed for {} op={}", topicFqn, event.opId(), t);
                          ackFail(event, "transition poll error: " + ThrowableUtils.rootMessage(t));
                          return;
                      }
                      if (outcome.isEmpty()) {
                          ackFail(event, versionWaitTimeoutMessage(targetVersion, topicFqn));
                          return;
                      }
                      onVersionResolved(event, outcome.get());
                  });
    }

    private String versionWaitTimeoutMessage(long targetVersion, String topicFqn) {
        return "timeout awaiting topic version " + targetVersion + " (current " + describe(currentVersion(topicFqn))
               + ")";
    }

    /**
     * Terminal when the cache has reached (or overshot) the coordinated version, empty while it is
     * still behind or absent (keep polling). Returns the observed version on termination.
     */
    private Optional<Long> probeVersion(String topicFqn, long targetVersion) {
        Optional<Long> current = currentVersion(topicFqn);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        long version = current.get();
        if (version >= targetVersion) {
            return Optional.of(version);
        }
        return Optional.empty();
    }

    private void onVersionResolved(TransitionEvent event, long current) {
        if (current > event.topicVersionToAwait()) {
            // The cache jumped past the version the controller coordinated: the topic was modified
            // concurrently during the transition. Fail so the controller can abort/retry rather than
            // act on a version it never coordinated.
            ackFail(
                event,
                "topic version overshot target " + event.topicVersionToAwait() + " (current " + current
                       + "), concurrent modification"
            );
            return;
        }
        Optional<Resource.EntityResource<VaradhiTopic>> cached = topicCache.get(event.topicFqn().toFqn());
        if (cached.isEmpty()) {
            ackFail(event, "topic absent from cache after version convergence");
            return;
        }
        onVersionReached(event, cached.get().getEntity());
    }

    private Optional<Long> currentVersion(String topicFqn) {
        return topicCache.get(topicFqn).map(Resource::getVersion).map(Integer::longValue);
    }

    private void onVersionReached(TransitionEvent event, VaradhiTopic topic) {
        // Only PREPARE runs participant work; every other version-gated stage just acks on convergence.
        if (event.stage() != TransitionStage.PREPARE) {
            ackOk(event);
            return;
        }
        if (!producerService.hasProducer(event.topicFqn())) {
            TransitionParticipation participation = TransitionParticipation.NOT_INVOLVED;
            recordParticipation(event, participation);
            log.debug(
                "Transition: pod not involved for {} op={} type={}; skipping participant work",
                event.topicFqn().toFqn(),
                event.opId(),
                event.transitionType()
            );
            ackOk(event, participation);
            return;
        }
        TransitionParticipation participation = TransitionParticipation.INVOLVED;
        recordParticipation(event, participation);
        createTarget(event, topic).whenComplete((ignored, t) -> {
            if (t != null) {
                log.warn(
                    "Transition PREPARE warm failed for {} op={} type={}",
                    event.topicFqn().toFqn(),
                    event.opId(),
                    event.transitionType(),
                    t
                );
                ackFail(event, participation, "prepare warm failed: " + ThrowableUtils.rootMessage(t));
                return;
            }
            ackOk(event, participation);
        });
    }

    /**
     * Type-specific PREPARE warm for an {@link TransitionParticipation#INVOLVED} pod.
     */
    private CompletableFuture<Void> createTarget(TransitionEvent event, VaradhiTopic topic) {
        TransitionEvent.Target target = event.target();
        if (target instanceof TransitionEvent.Target.Region regionTarget) {
            return producerService.getProducerForRegion(topic, regionTarget.region()).thenAccept(producer -> {});
        }
        if (target instanceof TransitionEvent.Target.StorageTopic storageTarget) {
            return producerService.loadProducer(event.topicFqn(), storageTarget.storageTopicId());
        }
        return CompletableFuture.failedFuture(new IllegalStateException("PREPARE target missing"));
    }

    private static String describe(Optional<Long> version) {
        return version.map(Object::toString).orElse("absent from cache");
    }

    private void recordParticipation(TransitionEvent event, TransitionParticipation participation) {
        participationByOpId.put(event.opId(), participation);
        metrics.setParticipation(event.transitionType(), participation);
    }

    /**
     * Participation decided at PREPARE and sticky for the op. Late joiners (first event not PREPARE)
     * derive from {@link ProducerService#hasProducer(VaradhiTopicName)}.
     */
    private TransitionParticipation resolveParticipation(TransitionEvent event) {
        return participationByOpId.computeIfAbsent(event.opId(), ignored -> deriveParticipation(event.topicFqn()));
    }

    private TransitionParticipation deriveParticipation(VaradhiTopicName topicFqn) {
        return producerService.hasProducer(topicFqn) ?
            TransitionParticipation.INVOLVED :
            TransitionParticipation.NOT_INVOLVED;
    }

    private void clearParticipationIfTerminal(TransitionEvent event) {
        if (event.stage() == TransitionStage.COMPLETED || event.stage() == TransitionStage.ABORTED) {
            participationByOpId.remove(event.opId());
            metrics.clearParticipation(event.transitionType());
        }
    }

    private void ackOk(TransitionEvent event) {
        ackOk(event, resolveParticipation(event));
    }

    private void ackOk(TransitionEvent event, TransitionParticipation participation) {
        metrics.stageAcked(event.transitionType(), event.stage(), true);
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
        ackFail(event, resolveParticipation(event), errorMsg);
    }

    private void ackFail(TransitionEvent event, TransitionParticipation participation, String errorMsg) {
        metrics.stageAcked(event.transitionType(), event.stage(), false);
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
        controllerClient.ackTopicTransition(ack).exceptionally(t -> {
            metrics.ackSendFailed(ack.transitionType(), ack.stage());
            log.warn("Failed to deliver transition ack ack={}", ack, t);
            return null;
        });
    }
}

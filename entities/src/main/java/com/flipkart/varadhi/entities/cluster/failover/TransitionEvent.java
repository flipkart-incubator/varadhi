package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopicName;

/**
 * Immutable, self-contained payload the controller broadcasts to every pod when a topic
 * transition advances to a stage that needs pod participation. The same event drives every
 * {@link TransitionType} (topic failover, storage-topic migration); the pod reacts to
 * {@link #stage()} generically.
 *
 * <p><b>Self-contained by design:</b> a pod reacts using only the fields here plus what it
 * already has in its local {@code TopicCache}.
 *
 * <ul>
 *   <li>{@code transitionType} — which transition this event belongs to. Selects the
 *       PREPARE action the pod runs and the expected {@link Target} shape.</li>
 *   <li>{@code awaitVersion} — whether this stage is version-gated. When {@code true} the pod
 *       must observe {@code topicVersionToAwait} in its TopicCache before acking; when
 *       {@code false} the version carried is ignored and the pod acks immediately on receipt
 *       ({@link TransitionStage#PENDING}, {@link TransitionStage#COMPLETED},
 *       {@link TransitionStage#ABORTED}).</li>
 *   <li>{@code topicVersionToAwait} — the {@code VaradhiTopic} version the pod must observe in
 *       its TopicCache before acking.</li>
 *   <li>{@code target} — the destination of the switch as a {@link Target}.
 *       Carried on {@link TransitionStage#PREPARE} so the pod can pre-create the target producer
 *       ahead of the switch. {@code null} for stages that do not pre-warm.</li>
 * </ul>
 */
//explore 2 class
public record TransitionEvent(
    String opId,
    VaradhiTopicName topicFqn,
    TransitionType transitionType,
    TransitionStage stage,
    boolean awaitVersion,
    long topicVersionToAwait,
    Target target // make generic avoid type http util (line no. ) // see if jackson is doing it
) {

    /**
     * Typed PREPARE target, discriminated on the wire by {@code @targetType}.
     */
    @JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@targetType")
    @JsonSubTypes ({
        @JsonSubTypes.Type (value = Target.Region.class, name = "region"),
        @JsonSubTypes.Type (value = Target.StorageTopic.class, name = "storageTopic"),})
    public sealed interface Target permits Target.Region, Target.StorageTopic {

        record Region(RegionName region) implements Target {
        }


        record StorageTopic(int storageTopicId) implements Target {
        }
    }

    /**
     * Creates a stage broadcast for pods. {@code awaitVersion}, {@code topicVersionToAwait}, and
     * {@code target} are controller-driven wire fields — the controller decides per stage whether
     * pods must converge on a version and whether a pre-warm target is carried.
     */
    public static TransitionEvent of(
        String opId,
        VaradhiTopicName topicFqn,
        TransitionType transitionType,
        TransitionStage stage,
        boolean awaitVersion,
        long topicVersionToAwait,
        Target target
    ) {
        return new TransitionEvent(opId, topicFqn, transitionType, stage, awaitVersion, topicVersionToAwait, target);
    }
}

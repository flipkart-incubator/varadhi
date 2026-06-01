package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * Pointer-only L2 entity that records the existence of an in-flight failover for a
 * parent {@code Topic} (or, in future, {@code Subscription}). Its only purpose is:
 *
 * <ol>
 *   <li>Allow {@code GET /v1/.../failover} to discover the live failover Op for a parent
 *       without scanning every Op znode.</li>
 *   <li>Provide the read-side guard for {@code DELETE topic} ("topic has an active
 *       failover, abort it first") by simply checking ZK path existence.</li>
 * </ol>
 *
 * <p><b>Immutability contract:</b> once created the FTO is never updated; it is only
 * created (Phase 0, atomically with the Op) and deleted (COMPLETED / ABORTED). All
 * stage / fence / ack state lives on {@link TopicFailoverOperation}.
 *
 * <p>The {@link #getName()} is the same as the parent FQN, so the per-parent uniqueness
 * is enforced by ZK znode creation. Use {@link MetaStoreEntityType#TOPIC_FAILOVER} or
 * {@link MetaStoreEntityType#SUB_FAILOVER} for the entity type.
 */
@Getter
@ToString
@EqualsAndHashCode (callSuper = true)
public final class FailoverTransitionObject extends MetaStoreEntity {

    private final String parentFqn;
    private final String parentKind;
    private final String operationId;
    private final long startTime;

    @JsonCreator
    public FailoverTransitionObject(
        @JsonProperty ("name") String name,
        @JsonProperty ("version") int version,
        @JsonProperty ("parentFqn") String parentFqn,
        @JsonProperty ("parentKind") String parentKind,
        @JsonProperty ("operationId") String operationId,
        @JsonProperty ("startTime") long startTime
    ) {
        super(name, version, entityTypeFor(parentKind));
        this.parentFqn = Objects.requireNonNull(parentFqn, "parentFqn");
        this.parentKind = Objects.requireNonNull(parentKind, "parentKind");
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.startTime = startTime;
    }

    public static FailoverTransitionObject forTopic(String topicFqn, String operationId) {
        return new FailoverTransitionObject(
            topicFqn,
            0,
            topicFqn,
            "topic",
            operationId,
            System.currentTimeMillis()
        );
    }

    public static FailoverTransitionObject forSubscription(String subFqn, String operationId) {
        return new FailoverTransitionObject(subFqn, 0, subFqn, "subscription", operationId, System.currentTimeMillis());
    }

    @JsonIgnore
    public boolean isTopicScoped() {
        return "topic".equalsIgnoreCase(parentKind);
    }

    private static MetaStoreEntityType entityTypeFor(String parentKind) {
        if ("topic".equalsIgnoreCase(parentKind)) {
            return MetaStoreEntityType.TOPIC_FAILOVER;
        }
        if ("subscription".equalsIgnoreCase(parentKind)) {
            return MetaStoreEntityType.SUB_FAILOVER;
        }
        throw new IllegalArgumentException("Unknown parentKind: " + parentKind);
    }
}

package com.flipkart.varadhi.entities;

import lombok.Getter;

/**
 * Per-region produce gate on {@link ProduceConfig}. Replicated via {@code TopicCache}; each produce
 * pod checks the entry for its {@code deployedRegion}. Multiple regions may be {@link #Producing}
 * at once on a global topic.
 *
 * <p>When produce is rejected, {@link #getProduceStatus()} is returned to callers via
 * {@link com.flipkart.varadhi.produce.ProduceResult#ofNonProducingTopic}.
 */
@Getter
public enum TopicState {

    /**
     * This region accepts produce. Successful produces report {@link ProduceStatus#Success} from
     * the broker path (not from this enum).
     */
    Producing(true, ProduceStatus.Success),

    /**
     * Transient drain during a transition (e.g. failover PREPARE→SWITCH). Clients receive
     * {@link ProduceStatus#Fenced} and should retry after the transition completes.
     */
    Fenced(false, ProduceStatus.Fenced),

    /**
     * This region does not accept produce in steady state (standby, or drained after failover out).
     * Other regions may still be {@link #Producing}.
     */
    Blocked(false, ProduceStatus.NotAllowed);

    private final ProduceStatus produceStatus;
    private final boolean produceAllowed;

    TopicState(boolean produceAllowed, ProduceStatus status) {
        this.produceStatus = status;
        this.produceAllowed = produceAllowed;
    }
}

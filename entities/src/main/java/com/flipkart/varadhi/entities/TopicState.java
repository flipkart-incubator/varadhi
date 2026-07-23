package com.flipkart.varadhi.entities;

import lombok.Getter;

/**
 * Topic-level runtime produce gate carried on {@link VaradhiTopic#topicState}. Replicated to every
 * pod's {@code TopicCache}; {@link com.flipkart.varadhi.produce.ProducerService} checks
 * {@link #isProduceAllowed()} before sending to the messaging stack.
 *
 * <p>Distinct from {@link VaradhiTopic#getRegionConfigs()} ({@code produceAllowed} per region —
 * which region owns produce authority). {@code TopicState} is the fence/drain signal applied to the
 * topic entity during failover (e.g. SWITCH sets source to {@link #Fenced}).
 *
 * <p>When produce is blocked, {@link #getProduceStatus()} is the {@link ProduceStatus} returned to
 * callers via {@link com.flipkart.varadhi.produce.ProduceResult#ofNonProducingTopic}.
 */
@Getter
public enum TopicState {

    /**
     * Normal steady state — produce is allowed. Successful produces report
     * {@link ProduceStatus#Success} from the broker path (not from this enum).
     */
    Producing(true, ProduceStatus.Success),

    /**
     * Transition Operations fence — produce blocked while authority is switching. Clients receive
     * {@link ProduceStatus#Fenced} and should retry after the transition completes.
     */
    Fenced(false, ProduceStatus.Fenced);

    private final ProduceStatus produceStatus;
    private final boolean produceAllowed;

    TopicState(boolean produceAllowed, ProduceStatus status) {
        this.produceStatus = status;
        this.produceAllowed = produceAllowed;
    }
}

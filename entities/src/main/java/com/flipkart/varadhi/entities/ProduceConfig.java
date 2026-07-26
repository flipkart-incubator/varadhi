package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-region produce policy for a {@link VaradhiTopic}. Multiple regions may be
 * {@link TopicState#Producing} concurrently on a global topic; each pod gates produce using its
 * {@code deployedRegion}'s entry.
 */
public record ProduceConfig(TopicState state, RegionName failOverRegion) {

    /** Region that accepts produce. */
    public static ProduceConfig producing() {
        return new ProduceConfig(TopicState.Producing, null);
    }

    /** Region that does not accept produce (standby / drained). */
    public static ProduceConfig blocked() {
        return new ProduceConfig(TopicState.Blocked, null);
    }

    @JsonCreator
    static ProduceConfig fromJson(
        @JsonProperty ("state") TopicState state,
        @JsonProperty ("produceAllowed") Boolean produceAllowed,
        @JsonProperty ("failOverRegion") RegionName failOverRegion
    ) {
        if (state != null) {
            return new ProduceConfig(state, failOverRegion);
        }
        // Legacy RegionConfig wire shape: produceAllowed boolean.
        if (Boolean.TRUE.equals(produceAllowed)) {
            return new ProduceConfig(TopicState.Producing, failOverRegion);
        }
        return new ProduceConfig(TopicState.Blocked, failOverRegion);
    }
}

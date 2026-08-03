package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Optional;

/**
 * Per-region produce policy for a {@link VaradhiTopic}. Multiple regions may be
 * {@link TopicState#Producing} concurrently on a global topic; each pod gates produce using its
 * {@code deployedRegion}'s entry.
 *
 * <p>{@code failOverRegion} is set by the controller when this region's produce is routed elsewhere
 * (typically at SWITCH while this entry may still be {@link TopicState#Producing}); it must reference
 * a region present in the topic's {@code produceConfigs} (controller validates on write).
 */
public record ProduceConfig(TopicState state, RegionName failOverRegion) {

    @JsonIgnore
    public Optional<RegionName> getFailoverRegion() {
        return Optional.ofNullable(failOverRegion);
    }

    /** Region that accepts produce. */
    public static ProduceConfig producing() {
        return new ProduceConfig(TopicState.Producing, null);
    }

    /** Region that does not accept produce (standby / drained). */
    public static ProduceConfig blocked() {
        return new ProduceConfig(TopicState.Blocked, null);
    }
}

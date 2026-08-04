package com.flipkart.varadhi.entities;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Optional;

/**
 * Per-region produce policy for a {@link VaradhiTopic}. Multiple regions may be
 * {@link TopicState#Producing} concurrently on a global topic; each pod gates produce using its
 * {@code deployedRegion}'s entry.
 *
 * <p>{@code failOverRegion} is set by the controller when this region's produce is routed elsewhere
 * (typically at SWITCH while this entry may still be {@link TopicState#Producing}); it must reference
 * a region present in the topic's {@code produceConfigs} (controller validates on write).
 * {@code null} when produce stays in this region — use {@link #getFailOverRegion()}.
 *
 * <p>{@code produceIdx} selects the active slot in {@link SegmentedStorageTopic#getTopicAtIndex(int)}
 * for this region's produce path (partition-growth / storage migration). Topic failover does not
 * change it — region routing uses {@code failOverRegion}.
 *
 * <p>Not a record: the raw {@code failOverRegion} field must stay inaccessible; only the Optional
 * getter is exposed.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public final class ProduceConfig {

    private final TopicState state;
    @Getter (AccessLevel.NONE)
    private final RegionName failOverRegion;
    private final int produceIdx;

    /** Failover target region; empty when produce stays in this region. */
    public Optional<RegionName> getFailOverRegion() {
        return Optional.ofNullable(failOverRegion);
    }

    /** Region that accepts produce. */
    public static ProduceConfig producing() {
        return new ProduceConfig(TopicState.Producing, null, 0);
    }

    /** Region that does not accept produce (standby / drained). */
    public static ProduceConfig blocked() {
        return new ProduceConfig(TopicState.Blocked, null, 0);
    }
}

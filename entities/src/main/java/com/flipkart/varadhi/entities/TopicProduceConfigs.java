package com.flipkart.varadhi.entities;

import java.util.Objects;
import java.util.Optional;

/** Read helpers for {@link VaradhiTopic#getProduceConfigs()}. */
public final class TopicProduceConfigs {

    /**
     * Active produce region for {@code deployedRegion}: {@link ProduceConfig#failOverRegion()} when
     * set, otherwise {@code deployedRegion}. Empty when that region has no produce config.
     */
    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic, RegionName deployedRegion) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(deployedRegion, "deployedRegion must not be null");
        return topic.getProduceConfig(deployedRegion)
                    .map(config -> config.failOverRegion() != null ? config.failOverRegion() : deployedRegion);
    }
}

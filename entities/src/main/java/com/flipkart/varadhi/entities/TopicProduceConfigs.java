package com.flipkart.varadhi.entities;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Read helpers for {@link VaradhiTopic#getProduceConfigs()}. */
public final class TopicProduceConfigs {

    private TopicProduceConfigs() {
    }

    /**
     * Where produce goes for {@code deployedRegion}: {@link ProduceConfig#failOverRegion()} when set,
     * otherwise {@code deployedRegion}. Empty when that region has no produce config.
     */
    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic, RegionName deployedRegion) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(deployedRegion, "deployedRegion must not be null");
        return topic.getProduceConfig(deployedRegion)
                    .map(config -> config.failOverRegion() != null ? config.failOverRegion() : deployedRegion);
    }

    /**
     * Topic-wide active produce region: the sole produce-allowed deployed region, or that region's
     * {@link ProduceConfig#failOverRegion()} when set. Empty when none allow produce.
     */
    public static Optional<RegionName> findActiveProducingRegion(VaradhiTopic topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        RegionName producing = null;
        for (Map.Entry<RegionName, ProduceConfig> entry : topic.getProduceConfigs().entrySet()) {
            ProduceConfig config = entry.getValue();
            if (!config.state().isProduceAllowed()) {
                continue;
            }
            RegionName active = config.failOverRegion() != null ? config.failOverRegion() : entry.getKey();
            if (producing != null) {
                throw new IllegalStateException(
                    "multiple regions allow produce: " + producing.value() + " and " + active.value()
                );
            }
            producing = active;
        }
        return Optional.ofNullable(producing);
    }
}

package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Read/update helpers for {@link VaradhiTopic#getRegionConfigs()}. */
public final class TopicRegionConfigs {

    private TopicRegionConfigs() {
    }

    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        RegionName producing = null;
        for (Map.Entry<String, RegionConfig> entry : topic.getRegionConfigs().entrySet()) {
            if (entry.getValue().isProduceAllowed()) {
                if (producing != null) {
                    throw new IllegalStateException(
                        "multiple regions allow produce: " + producing.value() + " and " + entry.getKey()
                    );
                }
                producing = RegionName.of(entry.getKey());
            }
        }
        return Optional.ofNullable(producing);
    }

    public static VaradhiTopic withRegionConfigs(VaradhiTopic topic, Map<String, RegionConfig> configs) {
        Objects.requireNonNull(topic, "topic must not be null");
        return topic.copyWith(new HashMap<>(Objects.requireNonNull(configs, "configs must not be null")), null, null);
    }
}

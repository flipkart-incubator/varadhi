package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Read/update helpers for {@link VaradhiTopic#getRegionConfigs()}. */
public final class TopicRegionConfigs {

    private TopicRegionConfigs() {
    }

    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic) {
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
        return topic.copyWith(new HashMap<>(configs), null, null);
    }
}

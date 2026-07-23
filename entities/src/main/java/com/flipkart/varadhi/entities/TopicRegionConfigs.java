package com.flipkart.varadhi.entities;

import java.util.Map;
import java.util.Optional;

/** Read/update helpers for {@link VaradhiTopic#getRegionConfigs()}. */
public final class TopicRegionConfigs {

    private TopicRegionConfigs() {
    }

    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic) {
        RegionName producing = null;
        for (Map.Entry<RegionName, RegionConfig> entry : topic.getRegionConfigs().entrySet()) {
            if (entry.getValue().produceAllowed()) {
                if (producing != null) {
                    throw new IllegalStateException(
                        "multiple regions allow produce: " + producing.value() + " and " + entry.getKey().value()
                    );
                }
                producing = entry.getKey();
            }
        }
        return Optional.ofNullable(producing);
    }
}

package com.flipkart.varadhi.entities;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Read helpers for {@link VaradhiTopic#getProduceConfigs()}. */
public final class TopicProduceConfigs {

    private TopicProduceConfigs() {
    }

    public static Optional<RegionName> findProducingRegion(VaradhiTopic topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        RegionName producing = null;
        for (Map.Entry<RegionName, ProduceConfig> entry : topic.getProduceConfigs().entrySet()) {
            if (entry.getValue().state().isProduceAllowed()) {
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

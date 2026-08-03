package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

public final class VaradhiTopicTestUtils {

    private VaradhiTopicTestUtils() {
    }

    public static VaradhiTopic withProduceConfigs(VaradhiTopic topic, Map<RegionName, ProduceConfig> configs) {
        return withProduceConfigs(topic, configs, topic.isAutoFailover());
    }

    public static VaradhiTopic withProduceConfigs(
        VaradhiTopic topic,
        Map<RegionName, ProduceConfig> configs,
        boolean autoFailover
    ) {
        return topic.copyWith(new HashMap<>(configs), autoFailover);
    }
}

package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

public final class VaradhiTopicTestUtils {

    private VaradhiTopicTestUtils() {
    }

    public static VaradhiTopic withProduceConfigs(VaradhiTopic topic, Map<RegionName, ProduceConfig> configs) {
        return topic.copyWith(new HashMap<>(configs), topic.isAutoFailover());
    }
}

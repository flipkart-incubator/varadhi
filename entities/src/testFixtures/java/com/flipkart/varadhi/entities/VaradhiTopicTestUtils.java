package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

/** Test helpers for building {@link VaradhiTopic} instances. */
public final class VaradhiTopicTestUtils {

    private VaradhiTopicTestUtils() {
    }

    public static VaradhiTopic withRegionConfigs(VaradhiTopic topic, Map<RegionName, RegionConfig> configs) {
        return topic.copyWith(new HashMap<>(configs), topic.getTopicState(), topic.isAutoFailover());
    }
}

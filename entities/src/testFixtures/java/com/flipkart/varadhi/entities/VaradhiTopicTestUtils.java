package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

public final class VaradhiTopicTestUtils {

    private static final String DEFAULT_PROJECT = "project1";
    private static final String DEFAULT_TOPIC = "topic1";
    private static final TopicCapacityPolicy DEFAULT_CAPACITY = new TopicCapacityPolicy(100, 400, 2, 2);

    private VaradhiTopicTestUtils() {
    }

    /** Topic with shared storage and per-region produce policy. */
    public static VaradhiTopic topicWithStorageAndProduceConfigs(Map<RegionName, ProduceConfig> configs) {
        return topicWithStorageAndProduceConfigs(configs, false);
    }

    public static VaradhiTopic topicWithStorageAndProduceConfigs(
        Map<RegionName, ProduceConfig> configs,
        boolean autoFailover
    ) {
        return topic(SegmentedStorageTopic.of(new TestStorageTopic("t")), autoFailover, configs);
    }

    /** Topic with produce policy but no storage provisioned yet. */
    public static VaradhiTopic topicWithProduceConfigs(Map<RegionName, ProduceConfig> configs) {
        return topic(null, false, configs);
    }

    private static VaradhiTopic topic(
        SegmentedStorageTopic storage,
        boolean autoFailover,
        Map<RegionName, ProduceConfig> configs
    ) {
        return VaradhiTopic.of(
            DEFAULT_PROJECT,
            DEFAULT_TOPIC,
            false,
            DEFAULT_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            storage,
            autoFailover,
            new HashMap<>(configs)
        );
    }

    static final class TestStorageTopic extends StorageTopic {
        TestStorageTopic(String name) {
            super(0, name);
        }
    }
}

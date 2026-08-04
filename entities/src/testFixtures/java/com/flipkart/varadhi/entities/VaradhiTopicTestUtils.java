package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

public final class VaradhiTopicTestUtils {

    private static final String DEFAULT_PROJECT = "project1";
    private static final String DEFAULT_TOPIC = "topic1";
    private static final TopicCapacityPolicy DEFAULT_CAPACITY = new TopicCapacityPolicy(100, 400, 2, 2);

    private VaradhiTopicTestUtils() {
    }

    /** Minimal storage segment for tests / resource conversion. */
    public static SegmentedStorageTopic testStorage() {
        return SegmentedStorageTopic.of(new TestStorageTopic("t"));
    }

    /** Topic with shared storage and per-region produce policy. */
    public static VaradhiTopic topicWithStorageAndProduceConfigs(Map<RegionName, ProduceConfig> configs) {
        return topicWithStorageAndProduceConfigs(configs, false);
    }

    public static VaradhiTopic topicWithStorageAndProduceConfigs(
        Map<RegionName, ProduceConfig> configs,
        boolean autoFailover
    ) {
        return topic(testStorage(), autoFailover, configs);
    }

    /** Topic with produce policy and default test storage. */
    public static VaradhiTopic topicWithProduceConfigs(Map<RegionName, ProduceConfig> configs) {
        return topic(testStorage(), false, configs);
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

    public static final class TestStorageTopic extends StorageTopic {
        public TestStorageTopic(String name) {
            super(0, name);
        }
    }
}

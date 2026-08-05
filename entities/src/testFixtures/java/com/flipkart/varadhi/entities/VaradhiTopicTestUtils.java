package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    public static VaradhiTopic getNewTopic(Map<RegionName, ProduceConfig> configs) {
        return getNewTopic(configs, false);
    }

    public static VaradhiTopic getNewTopic(Map<RegionName, ProduceConfig> configs, boolean autoFailover) {
        return topic(testStorage(), autoFailover, configs);
    }

    /**
     * Returns the shared {@link VaradhiTopic#getSegmentedStorageTopic()} when {@code region} participates
     * in the topic and failover routing is consistent.
     *
     * <p>Checks {@code region} is registered via {@link VaradhiTopic#getProduceConfig(RegionName)}. When
     * {@link ProduceConfig#getFailOverRegion()} is set, the failover target must also be registered —
     * same rule as {@link TopicResolver}.
     */
    public static Optional<SegmentedStorageTopic> getSegmentedStorage(VaradhiTopic topic, RegionName region) {
        Optional<ProduceConfig> config = topic.getProduceConfig(region);
        if (config.isEmpty()) {
            return Optional.empty();
        }
        RegionName effectiveRegion = config.get().getFailOverRegion().orElse(region);
        if (topic.getProduceConfig(effectiveRegion).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(topic.getSegmentedStorageTopic());
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

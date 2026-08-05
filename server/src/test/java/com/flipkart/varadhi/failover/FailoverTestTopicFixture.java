package com.flipkart.varadhi.failover;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds multi-region {@link VaradhiTopic} fixtures for topic-failover integration tests.
 */
public final class FailoverTestTopicFixture {

    @EqualsAndHashCode (callSuper = true)
    public static final class FixtureStorageTopic extends StorageTopic {
        public FixtureStorageTopic(String name) {
            super(0, name);
        }
    }

    private FailoverTestTopicFixture() {
    }

    public static VaradhiTopic create(String project, String topicName, String... regions) {
        if (regions.length < 2) {
            throw new IllegalArgumentException("a multi-region topic needs at least 2 regions");
        }
        Map<RegionName, ProduceConfig> configs = new HashMap<>();
        for (int i = 0; i < regions.length; i++) {
            configs.put(RegionName.of(regions[i]), i == 0 ? ProduceConfig.producing() : ProduceConfig.blocked());
        }
        return VaradhiTopic.of(
            project,
            topicName,
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            SegmentedStorageTopic.of(new FixtureStorageTopic(VaradhiTopic.fqn(project, topicName))),
            false,
            configs
        );
    }
}

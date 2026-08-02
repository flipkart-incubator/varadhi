package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceConfigTest {

    @Test
    void withProduceConfig_registersCallerSuppliedConfig() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.withSegmentedStorageTopic(
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1"))
        )
                     .withProduceConfig(RegionName.of("r1"), ProduceConfig.producing())
                     .withProduceConfig(RegionName.of("r2"), ProduceConfig.blocked());

        assertEquals(TopicState.Producing, topic.getProduceConfig(RegionName.of("r1")).orElseThrow().state());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().state());
    }

    @Test
    void withProduceConfigs_allowsMultipleProducingRegions() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.withSegmentedStorageTopic(
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1"))
        )
                     .withProduceConfig(RegionName.of("r1"), ProduceConfig.producing())
                     .withProduceConfig(RegionName.of("r2"), ProduceConfig.blocked());

        VaradhiTopic updated = VaradhiTopicTestUtils.withProduceConfigs(
            topic,
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.producing())
        );

        assertTrue(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state().isProduceAllowed());
        assertTrue(updated.getProduceConfig(RegionName.of("r2")).orElseThrow().state().isProduceAllowed());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().state());
    }

}

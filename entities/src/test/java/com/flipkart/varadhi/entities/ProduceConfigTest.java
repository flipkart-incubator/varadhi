package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceConfigTest {

    @Test
    void withProduceRegion_firstProducingThenBlocked() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.withStorageTopic(SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")))
                     .withProduceRegion(RegionName.of("r1"))
                     .withProduceRegion(RegionName.of("r2"));

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
        topic = topic.withStorageTopic(SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")))
                     .withProduceRegion(RegionName.of("r1"))
                     .withProduceRegion(RegionName.of("r2"));

        VaradhiTopic updated = VaradhiTopicTestUtils.withProduceConfigs(
            topic,
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.producing())
        );

        assertTrue(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state().isProduceAllowed());
        assertTrue(updated.getProduceConfig(RegionName.of("r2")).orElseThrow().state().isProduceAllowed());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().state());
    }

}

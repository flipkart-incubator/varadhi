package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TopicRegionConfigsTest {

    @Test
    void findProducingRegion_returnsSoleProducer() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.addInternalTopic(
            RegionName.of("r1"),
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1"))
        );
        topic = topic.addInternalTopic(
            RegionName.of("r2"),
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2"))
        );

        assertEquals(RegionName.of("r1"), TopicRegionConfigs.findProducingRegion(topic).orElseThrow());
    }

    @Test
    void withRegionConfigs_replacesProducePolicy() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.addInternalTopic(
            RegionName.of("r1"),
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1"))
        );
        topic = topic.addInternalTopic(
            RegionName.of("r2"),
            SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2"))
        );

        VaradhiTopic updated = VaradhiTopicTestUtils.withRegionConfigs(
            topic,
            Map.of(RegionName.of("r1"), new RegionConfig(false, null), RegionName.of("r2"), RegionConfig.producing())
        );

        assertEquals(RegionName.of("r2"), TopicRegionConfigs.findProducingRegion(updated).orElseThrow());
        assertFalse(updated.getRegionConfig(RegionName.of("r1")).orElseThrow().produceAllowed());
        assertEquals(RegionName.of("r1"), TopicRegionConfigs.findProducingRegion(topic).orElseThrow());
    }
}

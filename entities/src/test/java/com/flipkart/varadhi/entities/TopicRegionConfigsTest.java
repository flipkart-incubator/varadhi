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
<<<<<<< HEAD
<<<<<<< HEAD
        topic = topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic = topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));
=======
        topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
        topic = topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic = topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)

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
<<<<<<< HEAD
<<<<<<< HEAD
        topic = topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic = topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));

        VaradhiTopic updated = TopicRegionConfigs.withRegionConfigs(
            topic,
            Map.of("r1", new RegionConfig(false, null), "r2", RegionConfig.producing())
=======
        topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));

        VaradhiTopic updated = TopicRegionConfigs.withRegionConfigs(
            topic,
            Map.of("r1", new RegionConfig(true, false, null), "r2", RegionConfig.replicatedProducing())
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
        topic = topic.addInternalTopic("r1", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r1")));
        topic = topic.addInternalTopic("r2", SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t.r2")));

        VaradhiTopic updated = TopicRegionConfigs.withRegionConfigs(
            topic,
            Map.of("r1", new RegionConfig(false, null), "r2", RegionConfig.producing())
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)
        );

        assertEquals(RegionName.of("r2"), TopicRegionConfigs.findProducingRegion(updated).orElseThrow());
        assertFalse(updated.getRegionConfig(RegionName.of("r1")).isProduceAllowed());
        assertEquals(RegionName.of("r1"), TopicRegionConfigs.findProducingRegion(topic).orElseThrow());
    }
}

package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TopicRegionConfigsTest {

    private VaradhiTopic topicWithRegions() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        return topic.withStorageTopic(SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t")))
                    .withProduceRegion(RegionName.of("r1"))
                    .withProduceRegion(RegionName.of("r2"));
    }

    @Test
    void findProducingRegion_returnsSoleProducer() {
        VaradhiTopic topic = topicWithRegions();

        assertEquals(RegionName.of("r1"), TopicRegionConfigs.findProducingRegion(topic).orElseThrow());
    }

    @Test
    void findProducingRegion_reflectsSwitchedProducer() {
        VaradhiTopic topic = topicWithRegions();

        VaradhiTopic updated = topic.withProduceConfig(RegionName.of("r1"), ProduceConfig.blocked())
                                    .withProduceConfig(RegionName.of("r2"), ProduceConfig.producing());

        assertEquals(RegionName.of("r2"), TopicRegionConfigs.findProducingRegion(updated).orElseThrow());
        assertFalse(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state().isProduceAllowed());
        assertEquals(RegionName.of("r1"), TopicRegionConfigs.findProducingRegion(topic).orElseThrow());
    }
}

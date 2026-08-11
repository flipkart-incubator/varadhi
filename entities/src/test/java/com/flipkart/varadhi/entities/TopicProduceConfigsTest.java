package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicProduceConfigsTest {

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
    void findProducingRegion_returnsDeployedWhenNoFailOver() {
        VaradhiTopic topic = topicWithRegions();

        assertEquals(
            RegionName.of("r1"),
            TopicProduceConfigs.findProducingRegion(topic, RegionName.of("r1")).orElseThrow()
        );
        assertEquals(
            RegionName.of("r2"),
            TopicProduceConfigs.findProducingRegion(topic, RegionName.of("r2")).orElseThrow()
        );
    }

    @Test
    void findProducingRegion_usesFailOverRegionWhenSet() {
        VaradhiTopic topic = topicWithRegions().withProduceConfig(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Producing, RegionName.of("r2"))
        );

        assertEquals(
            RegionName.of("r2"),
            TopicProduceConfigs.findProducingRegion(topic, RegionName.of("r1")).orElseThrow()
        );
    }

    @Test
    void findProducingRegion_emptyWhenRegionMissing() {
        VaradhiTopic topic = topicWithRegions();

        assertTrue(TopicProduceConfigs.findProducingRegion(topic, RegionName.of("r3")).isEmpty());
    }

    @Test
    void findActiveProducingRegion_returnsSoleProducer() {
        VaradhiTopic topic = topicWithRegions();

        assertEquals(RegionName.of("r1"), TopicProduceConfigs.findActiveProducingRegion(topic).orElseThrow());
    }

    @Test
    void findActiveProducingRegion_usesFailOverWhenSet() {
        VaradhiTopic topic = topicWithRegions().withProduceConfig(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Producing, RegionName.of("r2"))
        );

        assertEquals(RegionName.of("r2"), TopicProduceConfigs.findActiveProducingRegion(topic).orElseThrow());
    }

    @Test
    void findActiveProducingRegion_reflectsSwitchedProducer() {
        VaradhiTopic topic = topicWithRegions().withProduceConfig(RegionName.of("r1"), ProduceConfig.blocked())
                                               .withProduceConfig(RegionName.of("r2"), ProduceConfig.producing());

        assertEquals(RegionName.of("r2"), TopicProduceConfigs.findActiveProducingRegion(topic).orElseThrow());
    }
}

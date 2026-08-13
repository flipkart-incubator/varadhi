package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicProduceConfigsTest {

    private VaradhiTopic topicWithRegions() {
        return VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );
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
        VaradhiTopic topic = topicWithRegions().with(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2"))
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
        VaradhiTopic topic = topicWithRegions().with(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2"))
        );

        assertEquals(RegionName.of("r2"), TopicProduceConfigs.findActiveProducingRegion(topic).orElseThrow());
    }

    @Test
    void findActiveProducingRegion_reflectsSwitchedProducer() {
        VaradhiTopic topic = topicWithRegions().with(RegionName.of("r1"), ProduceConfig.blocked())
                                               .with(RegionName.of("r2"), ProduceConfig.producing());

        assertEquals(RegionName.of("r2"), TopicProduceConfigs.findActiveProducingRegion(topic).orElseThrow());
    }
}

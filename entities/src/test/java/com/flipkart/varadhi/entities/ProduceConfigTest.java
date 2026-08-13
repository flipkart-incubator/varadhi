package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceConfigTest {

    @Test
    void producingAndBlockedFactories() {
        assertEquals(TopicState.Producing, ProduceConfig.producing().getState());
        assertEquals(TopicState.Blocked, ProduceConfig.blocked().getState());
        assertTrue(ProduceConfig.producing().getFailOverRegion().isEmpty());
    }

    @Test
    void withRegion_allowsMultipleProducingRegions() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );

        VaradhiTopic updated = topic.with(RegionName.of("r2"), ProduceConfig.producing());

        assertTrue(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState().isProduceAllowed());
        assertTrue(updated.getProduceConfig(RegionName.of("r2")).orElseThrow().getState().isProduceAllowed());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().getState());
    }

    @Test
    void failOverRegionOptional() {
        ProduceConfig cfg = new ProduceConfig(TopicState.Producing, 0, RegionName.of("CH"));
        assertEquals(RegionName.of("CH"), cfg.getFailOverRegion().orElseThrow());
    }
}

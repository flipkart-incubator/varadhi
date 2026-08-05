package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceConfigTest {

    @Test
    void withRegion_setsProducePolicy() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), ProduceConfig.producing()));
        VaradhiTopic updated = topic.with(RegionName.of("r2"), ProduceConfig.blocked());

        assertEquals(TopicState.Producing, updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState());
        assertEquals(TopicState.Blocked, updated.getProduceConfig(RegionName.of("r2")).orElseThrow().getState());
    }

    @Test
    void multipleProducingRegions_allowed() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of("r1"), ProduceConfig.producing(),
                RegionName.of("r2"), ProduceConfig.producing()
            )
        );

        assertTrue(topic.getProduceConfig(RegionName.of("r1")).orElseThrow().getState().isProduceAllowed());
        assertTrue(topic.getProduceConfig(RegionName.of("r2")).orElseThrow().getState().isProduceAllowed());
    }
}

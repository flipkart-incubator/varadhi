package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiTopicSerializationTest {

    @Test
    void jsonRoundTrip_preservesAutoFailoverAndRegionConfigs() {
        VaradhiTopic original = TopicRegionConfigs.withRegionConfigs(
            VaradhiTopic.of(
                "project1",
                "topic1",
                false,
                new TopicCapacityPolicy(100, 400, 2, 2),
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            ).withAutoFailover(true),
<<<<<<< HEAD
            Map.of("CH", RegionConfig.producing(), "HYD", new RegionConfig(false, RegionName.of("CH")))
=======
            Map.of("CH", new RegionConfig(true, true, null), "HYD", new RegionConfig(true, false, RegionName.of("CH")))
>>>>>>> de7413df (added regionConfig in varadhiTopic)
        );

        VaradhiTopic restored = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), VaradhiTopic.class);

        assertAll(
            () -> assertTrue(restored.isAutoFailover()),
            () -> assertEquals(2, restored.getRegionConfigs().size()),
            () -> assertTrue(restored.getRegionConfig(RegionName.of("CH")).isProduceAllowed()),
            () -> assertFalse(restored.getRegionConfig(RegionName.of("HYD")).isProduceAllowed()),
            () -> assertEquals(RegionName.of("CH"), restored.getRegionConfig(RegionName.of("HYD")).getFailOverRegion()),
            () -> assertEquals(RegionName.of("CH"), TopicRegionConfigs.findProducingRegion(restored).orElseThrow())
        );
    }
}

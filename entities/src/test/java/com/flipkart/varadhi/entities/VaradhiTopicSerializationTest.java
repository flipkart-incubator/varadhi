package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiTopicSerializationTest {

    private static final TopicCapacityPolicy CAPACITY = new TopicCapacityPolicy(100, 400, 2, 2);

    @Test
    void jsonRoundTrip_preservesAutoFailoverAndProduceConfigs() {
        Map<RegionName, ProduceConfig> configs = Map.of(
            RegionName.of("CH"),
            ProduceConfig.producing(),
            RegionName.of("HYD"),
            new ProduceConfig(TopicState.Blocked, Optional.of(RegionName.of("CH"))),
            RegionName.of("SIN"),
            ProduceConfig.producing()
        );

        VaradhiTopic original = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            null,
            true,
            configs
        );

        VaradhiTopic restored = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), VaradhiTopic.class);

        assertAll(
            () -> assertTrue(restored.isAutoFailover()),
            () -> assertTrue(restored.getProduceConfig(RegionName.of("CH")).isPresent()),
            () -> assertTrue(restored.getProduceConfig(RegionName.of("HYD")).isPresent()),
            () -> assertTrue(restored.getProduceConfig(RegionName.of("SIN")).isPresent()),
            () -> assertEquals(
                TopicState.Producing,
                restored.getProduceConfig(RegionName.of("CH")).orElseThrow().state()
            ),
            () -> assertEquals(
                TopicState.Blocked,
                restored.getProduceConfig(RegionName.of("HYD")).orElseThrow().state()
            ),
            () -> assertEquals(
                TopicState.Producing,
                restored.getProduceConfig(RegionName.of("SIN")).orElseThrow().state()
            ),
            () -> assertEquals(
                RegionName.of("CH"),
                restored.getProduceConfig(RegionName.of("HYD")).orElseThrow().failOverRegion().orElseThrow()
            )
        );
    }

    @Test
    void deserialize_produceConfigsWireShape() {
        VaradhiTopic original = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            null,
            true,
            Map.of(
                RegionName.of("CH"),
                ProduceConfig.producing(),
                RegionName.of("HYD"),
                new ProduceConfig(TopicState.Blocked, Optional.of(RegionName.of("CH")))
            )
        );

        String json = JsonMapper.jsonSerialize(original);
        assertTrue(json.contains("\"produceConfigs\""));
        assertTrue(!json.contains("\"regionConfigs\""));

        VaradhiTopic restored = JsonMapper.jsonDeserialize(json, VaradhiTopic.class);

        assertAll(
            () -> assertEquals(
                TopicState.Producing,
                restored.getProduceConfig(RegionName.of("CH")).orElseThrow().state()
            ),
            () -> assertEquals(
                TopicState.Blocked,
                restored.getProduceConfig(RegionName.of("HYD")).orElseThrow().state()
            ),
            () -> assertEquals(
                RegionName.of("CH"),
                restored.getProduceConfig(RegionName.of("HYD")).orElseThrow().failOverRegion().orElseThrow()
            )
        );
    }
}

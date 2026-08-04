package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceKeyResolverTest {

    private static final String PROJECT_NAME = "project1";
    private static final String TOPIC_NAME = "topic1";

    @Test
    void resolveForProduce_emptyWhenBlocked() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.blocked())
        );

        assertTrue(ProduceKeyResolver.resolve(topic, RegionName.of("r1"), true).isEmpty());
    }

    @Test
    void resolve_stillResolvesWhenFenced() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Fenced, Optional.of(RegionName.of("r2"))),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        assertTrue(ProduceKeyResolver.resolve(topic, RegionName.of("r1"), true).isEmpty());
        ProduceKey key = ProduceKeyResolver.resolve(topic, RegionName.of("r1")).orElseThrow();
        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }

    @Test
    void resolveForProduce_usesFailOverRegionAsProduceKey() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Producing, Optional.of(RegionName.of("r2"))),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        ProduceKey key = ProduceKeyResolver.resolve(topic, RegionName.of("r1"), true).orElseThrow();

        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }
}

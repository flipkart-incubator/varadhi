package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicResolverTest {

    private static final String PROJECT_NAME = "project1";
    private static final String TOPIC_NAME = "topic1";

    @Test
    void resolve_emptyWhenRegionUnknown() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), ProduceConfig.producing()));

        assertTrue(TopicResolver.resolve(topic, RegionName.of("unknown")).isEmpty());
    }

    @Test
    void resolve_emptyWhenFailoverRegionNotConfigured() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2")))
        );

        assertTrue(TopicResolver.resolve(topic, RegionName.of("r1")).isEmpty());
    }

    @Test
    void resolve_stillResolvesWhenBlockedForCacheWarm() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), ProduceConfig.blocked()));

        assertTrue(TopicResolver.resolve(topic, RegionName.of("r1"), true).isEmpty());
        ProduceKey key = TopicResolver.resolve(topic, RegionName.of("r1")).orElseThrow();
        assertEquals(RegionName.of("r1"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
    }

    @Test
    void resolve_throwsWhenStorageSegmentIdMissing() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, 99, null))
        );

        assertThrows(IllegalArgumentException.class, () -> TopicResolver.resolve(topic, RegionName.of("r1")));
    }

    @Test
    void resolve_stillResolvesWhenFenced() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Fenced, 0, RegionName.of("r2")),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        assertTrue(TopicResolver.resolve(topic, RegionName.of("r1"), true).isEmpty());
        ProduceKey key = TopicResolver.resolve(topic, RegionName.of("r1")).orElseThrow();
        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }

    @Test
    void resolveForProduce_usesFailOverRegionAsProduceKey() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2")),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        ProduceKey key = TopicResolver.resolve(topic, RegionName.of("r1"), true).orElseThrow();

        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }
}

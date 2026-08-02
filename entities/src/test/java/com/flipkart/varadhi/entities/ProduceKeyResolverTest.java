package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceKeyResolverTest {

    private static final String PROJECT_NAME = "project1";
    private static final String TOPIC_NAME = "topic1";

    private VaradhiTopic topicWithRegions(RegionName... regions) {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        ).withSegmentedStorageTopic(SegmentedStorageTopic.of(new VaradhiTopicTest.DummyStorageTopic("t")));
        for (RegionName region : regions) {
            topic = topic.withProduceConfig(region, ProduceConfig.producing());
        }
        return topic;
    }

    @Test
    void resolveForProduce_emptyWhenBlocked() {
        VaradhiTopic topic = topicWithRegions(RegionName.of("r1"));
        topic = topic.withProduceConfig(RegionName.of("r1"), ProduceConfig.blocked());

        assertTrue(ProduceKeyResolver.resolveForProduce(topic, RegionName.of("r1")).isEmpty());
    }

    @Test
    void resolve_stillResolvesWhenFenced() {
        VaradhiTopic topic = topicWithRegions(RegionName.of("r1"), RegionName.of("r2"));
        topic = topic.withProduceConfig(RegionName.of("r1"), new ProduceConfig(TopicState.Fenced, RegionName.of("r2")));

        assertTrue(ProduceKeyResolver.resolveForProduce(topic, RegionName.of("r1")).isEmpty());
        ProduceKey key = ProduceKeyResolver.resolve(topic, RegionName.of("r1")).orElseThrow();
        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }

    @Test
    void resolveForProduce_usesFailOverRegionAsProduceKey() {
        VaradhiTopic topic = topicWithRegions(RegionName.of("r1"), RegionName.of("r2"));
        topic = topic.withProduceConfig(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Producing, RegionName.of("r2"))
        );

        ProduceKey key = ProduceKeyResolver.resolveForProduce(topic, RegionName.of("r1")).orElseThrow();

        assertEquals(RegionName.of("r2"), key.produceRegion());
        assertEquals(0, key.storageTopicId());
        assertEquals(VaradhiTopicName.of(PROJECT_NAME, TOPIC_NAME), key.topicFqn());
    }
}

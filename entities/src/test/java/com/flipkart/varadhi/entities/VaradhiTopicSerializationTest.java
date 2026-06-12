package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiTopicSerializationTest {

    @Test
    void jsonDeserialize_OldStoredJsonWithoutNewFields_Succeeds() {
        String legacyJson = """
            {
              "name": "project1.topic1",
              "version": 1,
              "entityType": "TOPIC",
              "grouped": false,
              "capacity": {
                "qps": 100,
                "throughputKBps": 400,
                "readFanOut": 2,
                "retentionPeriodInDays": 2
              },
              "internalTopics": {},
              "status": {
                "state": "CREATED",
                "actionCode": "SYSTEM_ACTION"
              },
              "nfrFilterName": null,
              "topicCategory": "TOPIC"
            }
            """;

        VaradhiTopic topic = JsonMapper.jsonDeserialize(legacyJson, VaradhiTopic.class);

        assertAll(
            () -> assertEquals("project1.topic1", topic.getName()),
            () -> assertTrue(topic.getProduceRegionWeights().isEmpty()),
            () -> assertNull(topic.getMessageSizeProfile()),
            () -> assertNull(topic.getRateLimiterMode())
        );
    }

    @Test
    void jsonRoundTrip_PreservesNewFields() {
        VaradhiTopic topic = VaradhiTopic.of(
            "project1",
            "topic1",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            Map.of("region-a", 0.6, "region-b", 0.4),
            new MessageSizeProfile(512, 2048),
            RateLimiterMode.shadow
        );
        topic.markCreated();

        VaradhiTopic deserialized = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(topic), VaradhiTopic.class);

        assertAll(
            () -> assertNotNull(deserialized.getProduceRegionWeights()),
            () -> assertEquals(0.6, deserialized.getProduceRegionWeights().get("region-a")),
            () -> assertEquals(0.4, deserialized.getProduceRegionWeights().get("region-b")),
            () -> assertEquals(512, deserialized.getMessageSizeProfile().getAvgMsgSizeBytes()),
            () -> assertEquals(2048, deserialized.getMessageSizeProfile().getMaxMsgSizeBytes()),
            () -> assertEquals(RateLimiterMode.shadow, deserialized.getRateLimiterMode())
        );
    }
}

package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}

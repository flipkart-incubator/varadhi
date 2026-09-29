package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class TransitionEventSerializationTest {

    private static final String OP_ID = "op-1";
    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic");

    @Test
    void roundTrip_topicFailover_preservesRegionTarget() {
        TransitionEvent original = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            true,
            10L,
            new TransitionEvent.Target.Region(new RegionName("region-b"))
        );

        TransitionEvent restored = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(original),
            TransitionEvent.class
        );

        assertEquals(TransitionType.TOPIC_FAILOVER, restored.transitionType());
        assertInstanceOf(TransitionEvent.Target.Region.class, restored.target());
        assertEquals(new RegionName("region-b"), ((TransitionEvent.Target.Region)restored.target()).region());
    }

    @Test
    void roundTrip_storageMigration_preservesStorageTopicId() {
        TransitionEvent original = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.STORAGE_MIGRATION,
            TransitionStage.PREPARE,
            true,
            10L,
            new TransitionEvent.Target.StorageTopic(7)
        );

        TransitionEvent restored = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(original),
            TransitionEvent.class
        );

        assertEquals(TransitionType.STORAGE_MIGRATION, restored.transitionType());
        assertInstanceOf(TransitionEvent.Target.StorageTopic.class, restored.target());
        assertEquals(7, ((TransitionEvent.Target.StorageTopic)restored.target()).storageTopicId());
    }

    @Test
    void deserialize_regionTargetFromWireJson() {
        String json = """
            {
              "opId": "op-1",
              "topicFqn": {
                "projectName": "proj",
                "topicName": "topic"
              },
              "transitionType": "TOPIC_FAILOVER",
              "stage": "PREPARE",
              "awaitVersion": true,
              "topicVersionToAwait": 10,
              "target": {
                "@targetType": "region",
                "region": "region-b"
              }
            }
            """;

        TransitionEvent restored = JsonMapper.jsonDeserialize(json, TransitionEvent.class);

        assertInstanceOf(TransitionEvent.Target.Region.class, restored.target());
        assertEquals(new RegionName("region-b"), ((TransitionEvent.Target.Region)restored.target()).region());
    }

    @Test
    void deserialize_storageTopicTargetFromWireJson() {
        String json = """
            {
              "opId": "op-1",
              "topicFqn": {
                "projectName": "proj",
                "topicName": "topic"
              },
              "transitionType": "STORAGE_MIGRATION",
              "stage": "PREPARE",
              "awaitVersion": true,
              "topicVersionToAwait": 10,
              "target": {
                "@targetType": "storageTopic",
                "storageTopicId": 7
              }
            }
            """;

        TransitionEvent restored = JsonMapper.jsonDeserialize(json, TransitionEvent.class);

        assertInstanceOf(TransitionEvent.Target.StorageTopic.class, restored.target());
        assertEquals(7, ((TransitionEvent.Target.StorageTopic)restored.target()).storageTopicId());
    }

    @Test
    void deserialize_nullTarget() {
        String json = """
            {
              "opId": "op-1",
              "topicFqn": {
                "projectName": "proj",
                "topicName": "topic"
              },
              "transitionType": "TOPIC_FAILOVER",
              "stage": "FENCE",
              "awaitVersion": true,
              "topicVersionToAwait": 11,
              "target": null
            }
            """;

        TransitionEvent restored = JsonMapper.jsonDeserialize(json, TransitionEvent.class);

        assertNull(restored.target());
    }
}

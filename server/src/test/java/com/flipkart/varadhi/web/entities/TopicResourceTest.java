package com.flipkart.varadhi.web.entities;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicResourceTest {

    @Test
    void grouped_CreatesGroupedTopicResource() {
        TopicResource topicResource = TopicResource.grouped(
                "topicName", "projectName", new TopicCapacityPolicy(),
                LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        assertAll(
                () -> assertEquals("topicName", topicResource.getName()),
                () -> assertEquals("projectName", topicResource.getProject()),
                () -> assertTrue(topicResource.isGrouped()),
                () -> assertNotNull(topicResource.getCapacity()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, topicResource.getActorCode())
        );
    }

    @Test
    void unGrouped_CreatesUngroupedTopicResource() {
        TopicResource topicResource = TopicResource.unGrouped(
                "topicName", "projectName", new TopicCapacityPolicy(),
                LifecycleStatus.ActorCode.USER_ACTION
        );
        assertAll(
                () -> assertEquals("topicName", topicResource.getName()),
                () -> assertEquals("projectName", topicResource.getProject()),
                () -> assertFalse(topicResource.isGrouped()),
                () -> assertNotNull(topicResource.getCapacity()),
                () -> assertEquals(LifecycleStatus.ActorCode.USER_ACTION, topicResource.getActorCode())
        );
    }

    @Test
    void from_CreatesTopicResourceFromVaradhiTopic() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(
                "projectName", "topicName", true, new TopicCapacityPolicy(),
                LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        TopicResource topicResource = TopicResource.from(varadhiTopic);
        assertAll(
                () -> assertEquals("topicName", topicResource.getName()),
                () -> assertEquals("projectName", topicResource.getProject()),
                () -> assertTrue(topicResource.isGrouped()),
                () -> assertNotNull(topicResource.getCapacity()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, topicResource.getActorCode())
        );
    }

    @Test
    void toVaradhiTopic_ConvertsToVaradhiTopic() {
        TopicResource topicResource = TopicResource.grouped(
                "topicName", "projectName", new TopicCapacityPolicy(),
                LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();
        assertAll(
                () -> assertEquals("topicName", varadhiTopic.getName().split(NAME_SEPARATOR_REGEX)[1]),
                () -> assertEquals("projectName", varadhiTopic.getName().split(NAME_SEPARATOR_REGEX)[0]),
                () -> assertTrue(varadhiTopic.isGrouped()),
                () -> assertNotNull(varadhiTopic.getCapacity()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, varadhiTopic.getStatus().getActorCode())
        );
    }
}

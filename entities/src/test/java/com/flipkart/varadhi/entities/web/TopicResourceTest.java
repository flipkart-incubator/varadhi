package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicResourceTest {

    @Test
    void grouped_CreatesGroupedTopicResource() {
        TopicResource topicResource = TopicResource.grouped(
            "topicName",
            "projectName",
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        assertAll(
            () -> assertEquals("topicName", topicResource.getName()),
            () -> assertEquals("projectName", topicResource.getProject()),
            () -> assertTrue(topicResource.isGrouped()),
            () -> assertNotNull(topicResource.getCapacity()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, topicResource.getActionCode())
        );
    }

    @Test
    void unGrouped_CreatesUngroupedTopicResource() {
        TopicResource topicResource = TopicResource.unGrouped(
            "topicName",
            "projectName",
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.USER_ACTION,
            "test"
        );
        assertAll(
            () -> assertEquals("topicName", topicResource.getName()),
            () -> assertEquals("projectName", topicResource.getProject()),
            () -> assertFalse(topicResource.isGrouped()),
            () -> assertNotNull(topicResource.getCapacity()),
            () -> assertEquals(LifecycleStatus.ActionCode.USER_ACTION, topicResource.getActionCode())
        );
    }

    @Test
    void from_CreatesTopicResourceFromVaradhiTopic() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(
            "projectName",
            "topicName",
            true,
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        TopicResource topicResource = TopicResource.from(varadhiTopic);
        assertAll(
            () -> assertEquals("topicName", topicResource.getName()),
            () -> assertEquals("projectName", topicResource.getProject()),
            () -> assertTrue(topicResource.isGrouped()),
            () -> assertNotNull(topicResource.getCapacity()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, topicResource.getActionCode())
        );
    }

    @Test
    void toVaradhiTopic_ConvertsToVaradhiTopic() {
        TopicResource topicResource = TopicResource.grouped(
            "topicName",
            "projectName",
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();
        assertAll(
            () -> assertEquals("topicName", varadhiTopic.getName().split(NAME_SEPARATOR_REGEX)[1]),
            () -> assertEquals("projectName", varadhiTopic.getName().split(NAME_SEPARATOR_REGEX)[0]),
            () -> assertTrue(varadhiTopic.isGrouped()),
            () -> assertNotNull(varadhiTopic.getCapacity()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, varadhiTopic.getStatus().getActionCode())
        );
    }
}

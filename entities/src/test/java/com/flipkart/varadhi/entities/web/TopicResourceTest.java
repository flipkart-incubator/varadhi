package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void validate_RejectsBlankProject() {
        TopicResource topicResource = TopicResource.unGrouped(
            "topicName",
            "",
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.USER_ACTION,
            "test"
        );
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, topicResource::validate);
        assertTrue(ex.getMessage().contains("project"));
    }

    @Test
    void validate_RejectsNullProject() {
        TopicResource topicResource = TopicResource.unGrouped(
            "topicName",
            "projectName",
            new TopicCapacityPolicy(),
            LifecycleStatus.ActionCode.USER_ACTION,
            "test"
        );
        topicResource.setProject(null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, topicResource::validate);
        assertTrue(ex.getMessage().contains("project"));
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
            () -> assertEquals("topicName", varadhiTopic.getTopicName()),
            () -> assertEquals("projectName", varadhiTopic.getProjectName()),
            () -> assertTrue(varadhiTopic.isGrouped()),
            () -> assertNotNull(varadhiTopic.getCapacity()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, varadhiTopic.getStatus().getActionCode())
        );
    }
}

package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiTopicTest {

    private static final String PROJECT_NAME = "project1";
    private static final String TOPIC_NAME = "topic1";
    private static final TopicCapacityPolicy TOPIC_CAPACITY = new TopicCapacityPolicy(100, 400, 2);
    private static final String NFR_STRATEGY = "test";

    @EqualsAndHashCode (callSuper = true)
    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name) {
            super(name, TOPIC_CAPACITY);
        }
    }

    private VaradhiTopic createDefaultVaradhiTopic(boolean grouped) {
        return VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            grouped,
            TOPIC_CAPACITY,
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            NFR_STRATEGY
        );
    }

    @Test
    void of_WithValidInputs_CreatesVaradhiTopic() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);
        varadhiTopic.markCreated();

        assertAll(
            () -> assertEquals("project1.topic1", varadhiTopic.getName(), "Topic name mismatch"),
            () -> assertEquals(VaradhiTopic.INITIAL_VERSION, varadhiTopic.getVersion(), "Version mismatch"),
            () -> assertFalse(varadhiTopic.isGrouped(), "Grouped flag mismatch"),
            () -> assertEquals(TOPIC_CAPACITY, varadhiTopic.getCapacity(), "Capacity mismatch"),
            () -> assertTrue(varadhiTopic.isActive(), "Active status mismatch")
        );
    }

    @Test
    void of_WithGroupedFlag_CreatesGroupedVaradhiTopic() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(true);
        varadhiTopic.markCreated();

        assertAll(
            () -> assertEquals("project1.topic1", varadhiTopic.getName(), "Topic name mismatch"),
            () -> assertTrue(varadhiTopic.isGrouped(), "Grouped flag mismatch"),
            () -> assertEquals(TOPIC_CAPACITY, varadhiTopic.getCapacity(), "Capacity mismatch"),
            () -> assertTrue(varadhiTopic.isActive(), "Active status mismatch")
        );
    }

    @Test
    void buildTopicName_ReturnsExpectedFormat() {
        String expected = "project1.topic1";
        String actual = VaradhiTopic.buildTopicName(PROJECT_NAME, TOPIC_NAME);

        assertEquals(expected, actual, "Topic name format mismatch");
    }

    @Test
    void addInternalTopic_AddsTopicSuccessfully() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);
        StorageTopic storageTopic = new DummyStorageTopic(varadhiTopic.getName());

        varadhiTopic.addInternalTopic("region1", InternalCompositeTopic.of(storageTopic));

        assertEquals(
            storageTopic.getName(),
            varadhiTopic.getProduceTopicForRegion("region1").getTopicToProduce().getName(),
            "Internal topic addition failed"
        );
    }

    @Test
    void getProjectName_ReturnsCorrectProjectName() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        assertEquals(PROJECT_NAME, varadhiTopic.getProjectName(), "Project name mismatch");
    }

    @Test
    void getProduceTopicForRegion_WithValidRegion_ReturnsCorrectTopic() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);
        StorageTopic storageTopic = new DummyStorageTopic(varadhiTopic.getName());

        varadhiTopic.addInternalTopic("region1", InternalCompositeTopic.of(storageTopic));

        assertAll(
            () -> assertNotNull(varadhiTopic.getProduceTopicForRegion("region1"), "Region topic not found"),
            () -> assertEquals(
                storageTopic.getName(),
                varadhiTopic.getProduceTopicForRegion("region1").getTopicToProduce().getName(),
                "Region topic name mismatch"
            )
        );
    }

    @Test
    void getProduceTopicForRegion_WithUnknownRegion_ReturnsNull() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        assertNull(varadhiTopic.getProduceTopicForRegion("unknownRegion"), "Unknown region should return null");
    }

    @Test
    void restore_ChangeStateToCreated() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        varadhiTopic.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Deactivated");
        varadhiTopic.restore(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Activated");

        assertAll(
            () -> assertTrue(varadhiTopic.isActive(), "Active status update failed"),
            () -> assertEquals(
                LifecycleStatus.State.CREATED,
                varadhiTopic.getStatus().getState(),
                "Status state mismatch"
            ),
            () -> assertEquals(
                LifecycleStatus.ActorCode.SYSTEM_ACTION,
                varadhiTopic.getStatus().getActorCode(),
                "Action code mismatch"
            )
        );
    }

    @Test
    void markInactive_ChangesStatusToInactive() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        varadhiTopic.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Deactivated");

        assertAll(
            () -> assertFalse(varadhiTopic.isActive(), "Inactive status update failed"),
            () -> assertEquals(
                LifecycleStatus.State.INACTIVE,
                varadhiTopic.getStatus().getState(),
                "Status state mismatch"
            ),
            () -> assertEquals(
                LifecycleStatus.ActorCode.SYSTEM_ACTION,
                varadhiTopic.getStatus().getActorCode(),
                "Action code mismatch"
            )
        );
    }
}

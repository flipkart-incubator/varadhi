package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiTopicTest {

    private static final String PROJECT_NAME = "project1";
    private static final String TOPIC_NAME = "topic1";
    private static final TopicCapacityPolicy TOPIC_CAPACITY = new TopicCapacityPolicy(100, 400, 2, 2);

    @EqualsAndHashCode (callSuper = true)
    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name) {
            super(0, name);
        }
    }

    private VaradhiTopic createDefaultVaradhiTopic(boolean grouped) {
        return VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            grouped,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            VaradhiTopicTestUtils.testStorage(),
            false,
            Map.of()
        );
    }

    // --- Factory ---

    @Test
    void of_WithValidInputs_CreatesVaradhiTopic() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);
        varadhiTopic.markCreated();

        assertAll(
            () -> assertEquals("project1.topic1", varadhiTopic.getName(), "Topic name mismatch"),
            () -> assertEquals(VaradhiTopic.INITIAL_VERSION, varadhiTopic.getVersion(), "Version mismatch"),
            () -> assertFalse(varadhiTopic.isGrouped(), "Grouped flag mismatch"),
            () -> assertEquals(TOPIC_CAPACITY, varadhiTopic.getCapacity(), "Capacity mismatch"),
            () -> assertTrue(varadhiTopic.isActive(), "Active status mismatch"),
            () -> assertEquals(VaradhiTopic.TopicCategory.TOPIC, varadhiTopic.getTopicCategory()),
            () -> assertFalse(varadhiTopic.isAutoFailover()),
            () -> assertTrue(varadhiTopic.getProduceConfig(RegionName.of("unknown")).isEmpty())
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
    void of_fullFactory_setsStorageAutoFailoverProduceConfigsAndAuxFields() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));
        Map<RegionName, ProduceConfig> configs = Map.of(
            RegionName.of("r1"),
            ProduceConfig.producing(),
            RegionName.of("r2"),
            ProduceConfig.blocked()
        );
        MessageSizeProfile profile = new MessageSizeProfile(100, 1000);
        Map<String, Double> weights = Map.of("r1", 0.6, "r2", 0.4);

        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            true,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.USER_ACTION,
            "nfr-filter",
            VaradhiTopic.TopicCategory.TOPIC,
            weights,
            profile,
            RateLimiterMode.enforced,
            storage,
            true,
            configs
        );

        assertAll(
            () -> assertEquals("project1.topic1", topic.getName()),
            () -> assertTrue(topic.isGrouped()),
            () -> assertEquals("nfr-filter", topic.getNfrFilterName()),
            () -> assertEquals(weights, topic.getPerRegionQuotaWeights()),
            () -> assertEquals(profile, topic.getMessageSizeProfile()),
            () -> assertEquals(RateLimiterMode.enforced, topic.getRateLimiterMode()),
            () -> assertEquals(storage, topic.getSegmentedStorageTopic()),
            () -> assertTrue(topic.isAutoFailover()),
            () -> assertEquals(
                TopicState.Producing,
                topic.getProduceConfig(RegionName.of("r1")).orElseThrow().getState()
            ),
            () -> assertEquals(
                TopicState.Blocked,
                topic.getProduceConfig(RegionName.of("r2")).orElseThrow().getState()
            ),
            () -> assertEquals(LifecycleStatus.State.CREATING, topic.getStatus().getState())
        );
    }

    @Test
    void of_emptyProduceConfigs_andNonNullStorage() {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            VaradhiTopicTestUtils.testStorage(),
            false,
            Map.of()
        );

        assertAll(
            () -> assertNotNull(topic.getSegmentedStorageTopic()),
            () -> assertFalse(topic.isAutoFailover()),
            () -> assertTrue(topic.getProduceConfig(RegionName.of("r1")).isEmpty()),
            () -> assertTrue(topic.getPerRegionQuotaWeights().isEmpty())
        );
    }

    @Test
    void of_defensiveCopyOfProduceConfigs() {
        Map<RegionName, ProduceConfig> configs = new HashMap<>();
        configs.put(RegionName.of("r1"), ProduceConfig.producing());

        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            SegmentedStorageTopic.of(new DummyStorageTopic("t")),
            false,
            configs
        );

        configs.put(RegionName.of("r2"), ProduceConfig.blocked());

        assertTrue(topic.getProduceConfig(RegionName.of("r1")).isPresent());
        assertTrue(topic.getProduceConfig(RegionName.of("r2")).isEmpty());
    }

    @Test
    void fqn_ReturnsExpectedFormat() {
        assertEquals("project1.topic1", VaradhiTopic.fqn(PROJECT_NAME, TOPIC_NAME), "Topic name format mismatch");
    }

    // --- Read accessors ---

    @Test
    void getProjectName_ReturnsCorrectProjectName() {
        assertEquals(PROJECT_NAME, createDefaultVaradhiTopic(false).getProjectName(), "Project name mismatch");
    }

    @Test
    void getTopicName_ReturnsCorrectLocalTopicName() {
        assertEquals(TOPIC_NAME, createDefaultVaradhiTopic(false).getTopicName(), "Topic name segment mismatch");
    }

    @Test
    void getSegmentedStorage_WithValidRegion_ReturnsCorrectTopic() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("region1"), ProduceConfig.producing())
        );

        assertAll(
            () -> assertTrue(
                VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("region1")).isPresent(),
                "Region topic not found"
            ),
            () -> assertEquals(
                "t",
                VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("region1"))
                                     .orElseThrow()
                                     .getTopic(0)
                                     .getName(),
                "Region topic name mismatch"
            )
        );
    }

    @Test
    void getSegmentedStorage_WithUnknownRegion_ReturnsEmpty() {
        assertTrue(
            VaradhiTopicTestUtils.getSegmentedStorage(createDefaultVaradhiTopic(false), RegionName.of("unknownRegion"))
                                 .isEmpty()
        );
    }

    @Test
    void getSegmentedStorage_emptyWhenFailoverTargetNotRegistered() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2")))
        );

        assertTrue(VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("r1")).isEmpty());
    }

    @Test
    void getSegmentedStorage_presentWhenFailoverTargetRegistered() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2")),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        assertTrue(VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("r1")).isPresent());
    }

    @Test
    void getSegmentedStorage_presentForBlockedOrFencedRegion() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of("r1"),
                ProduceConfig.blocked(),
                RegionName.of("r2"),
                new ProduceConfig(TopicState.Fenced, 0, null)
            )
        );

        assertAll(
            () -> assertTrue(VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("r1")).isPresent()),
            () -> assertTrue(VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("r2")).isPresent()),
            () -> assertEquals(
                topic.getSegmentedStorageTopic(),
                VaradhiTopicTestUtils.getSegmentedStorage(topic, RegionName.of("r1")).orElseThrow()
            )
        );
    }

    @Test
    void getProduceConfig_returnsFailoverRegionWhenSet() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, 0, RegionName.of("r2")))
        );

        ProduceConfig config = topic.getProduceConfig(RegionName.of("r1")).orElseThrow();
        assertAll(
            () -> assertEquals(TopicState.Producing, config.getState()),
            () -> assertEquals(RegionName.of("r2"), config.getFailOverRegion().orElseThrow())
        );
    }

    @Test
    void produceConfigs_emptyUntilProduceRegionAdded() {
        assertTrue(createDefaultVaradhiTopic(false).getProduceConfig(RegionName.of("unknown")).isEmpty());
    }

    @Test
    void isCategory_WithMatchingCategory_ReturnsTrue() {
        assertTrue(createDefaultVaradhiTopic(false).isCategory(VaradhiTopic.TopicCategory.TOPIC));
    }

    @Test
    void isCategory_WithDifferentCategory_ReturnsFalse() {
        assertFalse(createDefaultVaradhiTopic(false).isCategory(VaradhiTopic.TopicCategory.QUEUE));
    }

    @Test
    void isCategory_WithQueueTopic_MatchesQueueCategory() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.QUEUE,
            null,
            null,
            null,
            VaradhiTopicTestUtils.testStorage(),
            false,
            Map.of()
        );

        assertAll(
            () -> assertTrue(varadhiTopic.isCategory(VaradhiTopic.TopicCategory.QUEUE)),
            () -> assertFalse(varadhiTopic.isCategory(VaradhiTopic.TopicCategory.TOPIC))
        );
    }

    // --- Lifecycle ---

    @Test
    void restore_ChangeStateToCreated() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        varadhiTopic.markInactive(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Deactivated");
        varadhiTopic.restore(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Activated");

        assertAll(
            () -> assertTrue(varadhiTopic.isActive(), "Active status update failed"),
            () -> assertEquals(
                LifecycleStatus.State.CREATED,
                varadhiTopic.getStatus().getState(),
                "Status state mismatch"
            ),
            () -> assertEquals(
                LifecycleStatus.ActionCode.SYSTEM_ACTION,
                varadhiTopic.getStatus().getActionCode(),
                "Action code mismatch"
            )
        );
    }

    @Test
    void markInactive_ChangesStatusToInactive() {
        VaradhiTopic varadhiTopic = createDefaultVaradhiTopic(false);

        varadhiTopic.markInactive(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Deactivated");

        assertAll(
            () -> assertFalse(varadhiTopic.isActive(), "Inactive status update failed"),
            () -> assertEquals(
                LifecycleStatus.State.INACTIVE,
                varadhiTopic.getStatus().getState(),
                "Status state mismatch"
            ),
            () -> assertEquals(
                LifecycleStatus.ActionCode.SYSTEM_ACTION,
                varadhiTopic.getStatus().getActionCode(),
                "Action code mismatch"
            )
        );
    }

    // --- Clone APIs (with* under test) ---

    @Test
    void with_registersRegionAndLeavesOriginalUnchanged() {
        VaradhiTopic original = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );

        VaradhiTopic updated = original.with(RegionName.of("r1"), new ProduceConfig(TopicState.Fenced, 0, null));

        assertEquals(TopicState.Fenced, updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState());
        assertEquals(
            TopicState.Producing,
            original.getProduceConfig(RegionName.of("r1")).orElseThrow().getState(),
            "original must be unchanged"
        );
    }

    @Test
    void with_registersMultipleRegions() {
        VaradhiTopic original = createDefaultVaradhiTopic(false);

        VaradhiTopic updated = original.with(RegionName.of("r1"), ProduceConfig.producing())
                                       .with(RegionName.of("r2"), ProduceConfig.blocked());

        assertEquals(TopicState.Producing, updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState());
        assertEquals(TopicState.Blocked, updated.getProduceConfig(RegionName.of("r2")).orElseThrow().getState());
        assertTrue(original.getProduceConfig(RegionName.of("r1")).isEmpty(), "original must be unchanged");
    }

    @Test
    void with_doesNotChangeStorage() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            storage,
            false,
            Map.of()
        );

        VaradhiTopic updated = topic.with(RegionName.of("r1"), ProduceConfig.producing());

        assertEquals(storage, updated.getSegmentedStorageTopic());
    }

    @Test
    void withSegmentedStorageTopic_replacesStoragePreservesProduceConfigs() {
        VaradhiTopic original = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));

        VaradhiTopic updated = original.with(storage);

        assertAll(
            () -> assertNotSame(original, updated),
            () -> assertNotNull(original.getSegmentedStorageTopic()),
            () -> assertEquals(storage, updated.getSegmentedStorageTopic()),
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState()
            ),
            () -> assertFalse(updated.isAutoFailover())
        );
    }

    @Test
    void withAndProduceConfig_setsBothInOneCopy() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));

        VaradhiTopic updated = createDefaultVaradhiTopic(false).with(
            storage,
            RegionName.of("r1"),
            ProduceConfig.producing()
        );

        assertAll(
            () -> assertEquals(storage, updated.getSegmentedStorageTopic()),
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState()
            ),
            () -> assertTrue(VaradhiTopicTestUtils.getSegmentedStorage(updated, RegionName.of("r1")).isPresent())
        );
    }

    @Test
    void withAndProduceConfig_preservesOtherRegions() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), ProduceConfig.producing()));

        VaradhiTopic updated = topic.with(storage, RegionName.of("r2"), ProduceConfig.blocked());

        assertAll(
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState()
            ),
            () -> assertEquals(
                TopicState.Blocked,
                updated.getProduceConfig(RegionName.of("r2")).orElseThrow().getState()
            ),
            () -> assertTrue(topic.getProduceConfig(RegionName.of("r2")).isEmpty(), "original unchanged")
        );
    }

    @Test
    void copyWith_replacesProduceConfigsWithoutMutatingOriginal() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );

        VaradhiTopic updated = topic.copyWith(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.producing()),
            topic.isAutoFailover()
        );

        assertTrue(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().getState().isProduceAllowed());
        assertTrue(updated.getProduceConfig(RegionName.of("r2")).orElseThrow().getState().isProduceAllowed());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().getState());
    }

    @Test
    void of_setsAutoFailover() {
        Map<RegionName, ProduceConfig> configs = Map.of(RegionName.of("r1"), ProduceConfig.producing());

        assertFalse(VaradhiTopicTestUtils.getNewTopic(configs).isAutoFailover());
        assertTrue(VaradhiTopicTestUtils.getNewTopic(configs, true).isAutoFailover());
    }

    @Test
    void produceConfig_factories() {
        assertAll(
            () -> assertEquals(TopicState.Producing, ProduceConfig.producing().getState()),
            () -> assertTrue(ProduceConfig.producing().getFailOverRegion().isEmpty()),
            () -> assertEquals(0, ProduceConfig.producing().getProduceIdx()),
            () -> assertEquals(TopicState.Blocked, ProduceConfig.blocked().getState()),
            () -> assertTrue(ProduceConfig.blocked().getFailOverRegion().isEmpty()),
            () -> assertEquals(0, ProduceConfig.blocked().getProduceIdx())
        );
    }
}

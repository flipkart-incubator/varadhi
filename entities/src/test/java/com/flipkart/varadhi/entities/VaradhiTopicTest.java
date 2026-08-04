package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            null
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
            () -> assertEquals(TopicState.Producing, topic.getProduceConfig(RegionName.of("r1")).orElseThrow().state()),
            () -> assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().state()),
            () -> assertEquals(LifecycleStatus.State.CREATING, topic.getStatus().getState())
        );
    }

    @Test
    void of_nullProduceConfigs_startsWithEmptyMap() {
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
            null,
            false,
            null
        );

        assertAll(
            () -> assertNull(topic.getSegmentedStorageTopic()),
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
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("region1"), ProduceConfig.producing())
        );

        assertAll(
            () -> assertTrue(topic.getSegmentedStorage(RegionName.of("region1")).isPresent(), "Region topic not found"),
            () -> assertEquals(
                "t",
                topic.getSegmentedStorage(RegionName.of("region1")).orElseThrow().getTopicToProduce().getName(),
                "Region topic name mismatch"
            )
        );
    }

    @Test
    void getSegmentedStorage_WithUnknownRegion_ReturnsEmpty() {
        assertTrue(createDefaultVaradhiTopic(false).getSegmentedStorage(RegionName.of("unknownRegion")).isEmpty());
    }

    @Test
    void getSegmentedStorage_emptyWhenFailoverTargetNotRegistered() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, Optional.of(RegionName.of("r2"))))
        );

        assertTrue(topic.getSegmentedStorage(RegionName.of("r1")).isEmpty());
    }

    @Test
    void getSegmentedStorage_presentWhenFailoverTargetRegistered() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(
                RegionName.of("r1"),
                new ProduceConfig(TopicState.Producing, Optional.of(RegionName.of("r2"))),
                RegionName.of("r2"),
                ProduceConfig.producing()
            )
        );

        assertTrue(topic.getSegmentedStorage(RegionName.of("r1")).isPresent());
    }

    @Test
    void getSegmentedStorage_emptyWhenStorageNotProvisioned() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );

        assertTrue(topic.getSegmentedStorage(RegionName.of("r1")).isEmpty());
    }

    @Test
    void getSegmentedStorage_presentForBlockedOrFencedRegion() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(
                RegionName.of("r1"),
                ProduceConfig.blocked(),
                RegionName.of("r2"),
                new ProduceConfig(TopicState.Fenced, Optional.empty())
            )
        );

        assertAll(
            () -> assertTrue(topic.getSegmentedStorage(RegionName.of("r1")).isPresent()),
            () -> assertTrue(topic.getSegmentedStorage(RegionName.of("r2")).isPresent()),
            () -> assertEquals(
                topic.getSegmentedStorageTopic(),
                topic.getSegmentedStorage(RegionName.of("r1")).orElseThrow()
            )
        );
    }

    @Test
    void getProduceConfig_returnsFailoverRegionWhenSet() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithProduceConfigs(
            Map.of(RegionName.of("r1"), new ProduceConfig(TopicState.Producing, Optional.of(RegionName.of("r2"))))
        );

        ProduceConfig config = topic.getProduceConfig(RegionName.of("r1")).orElseThrow();
        assertAll(
            () -> assertEquals(TopicState.Producing, config.state()),
            () -> assertEquals(RegionName.of("r2"), config.failOverRegion().orElseThrow())
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
            null,
            false,
            null
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
    void withProduceConfig_registersRegionAndLeavesOriginalUnchanged() {
        VaradhiTopic original = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );

        VaradhiTopic updated = original.withProduceConfig(
            RegionName.of("r1"),
            new ProduceConfig(TopicState.Fenced, Optional.empty())
        );

        assertEquals(TopicState.Fenced, updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state());
        assertEquals(
            TopicState.Producing,
            original.getProduceConfig(RegionName.of("r1")).orElseThrow().state(),
            "original must be unchanged"
        );
    }

    @Test
    void withProduceConfig_registersMultipleRegions() {
        VaradhiTopic original = createDefaultVaradhiTopic(false);

        VaradhiTopic updated = original.withProduceConfig(RegionName.of("r1"), ProduceConfig.producing())
                                       .withProduceConfig(RegionName.of("r2"), ProduceConfig.blocked());

        assertEquals(TopicState.Producing, updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state());
        assertEquals(TopicState.Blocked, updated.getProduceConfig(RegionName.of("r2")).orElseThrow().state());
        assertTrue(original.getProduceConfig(RegionName.of("r1")).isEmpty(), "original must be unchanged");
    }

    @Test
    void withProduceConfig_doesNotChangeStorage() {
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

        VaradhiTopic updated = topic.withProduceConfig(RegionName.of("r1"), ProduceConfig.producing());

        assertEquals(storage, updated.getSegmentedStorageTopic());
    }

    @Test
    void withSegmentedStorageTopic_replacesStoragePreservesProduceConfigs() {
        VaradhiTopic original = VaradhiTopicTestUtils.topicWithProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));

        VaradhiTopic updated = original.withSegmentedStorageTopic(storage);

        assertAll(
            () -> assertNotSame(original, updated),
            () -> assertNull(original.getSegmentedStorageTopic()),
            () -> assertEquals(storage, updated.getSegmentedStorageTopic()),
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state()
            ),
            () -> assertFalse(updated.isAutoFailover())
        );
    }

    @Test
    void withSegmentedStorageTopicAndProduceConfig_setsBothInOneCopy() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));

        VaradhiTopic updated = createDefaultVaradhiTopic(false).withSegmentedStorageTopicAndProduceConfig(
            storage,
            RegionName.of("r1"),
            ProduceConfig.producing()
        );

        assertAll(
            () -> assertEquals(storage, updated.getSegmentedStorageTopic()),
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state()
            ),
            () -> assertTrue(updated.getSegmentedStorage(RegionName.of("r1")).isPresent())
        );
    }

    @Test
    void withSegmentedStorageTopicAndProduceConfig_preservesOtherRegions() {
        SegmentedStorageTopic storage = SegmentedStorageTopic.of(new DummyStorageTopic("t"));
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.producing())
        );

        VaradhiTopic updated = topic.withSegmentedStorageTopicAndProduceConfig(
            storage,
            RegionName.of("r2"),
            ProduceConfig.blocked()
        );

        assertAll(
            () -> assertEquals(
                TopicState.Producing,
                updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state()
            ),
            () -> assertEquals(TopicState.Blocked, updated.getProduceConfig(RegionName.of("r2")).orElseThrow().state()),
            () -> assertTrue(topic.getProduceConfig(RegionName.of("r2")).isEmpty(), "original unchanged")
        );
    }

    @Test
    void copyWith_replacesProduceConfigsWithoutMutatingOriginal() {
        VaradhiTopic topic = VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );

        VaradhiTopic updated = topic.copyWith(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.producing()),
            topic.isAutoFailover()
        );

        assertTrue(updated.getProduceConfig(RegionName.of("r1")).orElseThrow().state().isProduceAllowed());
        assertTrue(updated.getProduceConfig(RegionName.of("r2")).orElseThrow().state().isProduceAllowed());
        assertEquals(TopicState.Blocked, topic.getProduceConfig(RegionName.of("r2")).orElseThrow().state());
    }

    @Test
    void of_setsAutoFailover() {
        Map<RegionName, ProduceConfig> configs = Map.of(RegionName.of("r1"), ProduceConfig.producing());

        assertFalse(VaradhiTopicTestUtils.topicWithProduceConfigs(configs).isAutoFailover());
        assertTrue(VaradhiTopicTestUtils.topicWithStorageAndProduceConfigs(configs, true).isAutoFailover());
    }

    @Test
    void produceConfig_factories() {
        assertAll(
            () -> assertEquals(TopicState.Producing, ProduceConfig.producing().state()),
            () -> assertTrue(ProduceConfig.producing().failOverRegion().isEmpty()),
            () -> assertEquals(TopicState.Blocked, ProduceConfig.blocked().state()),
            () -> assertTrue(ProduceConfig.blocked().failOverRegion().isEmpty())
        );
    }
}

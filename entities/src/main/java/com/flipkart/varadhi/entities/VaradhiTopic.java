package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a topic in the Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class VaradhiTopic extends LifecycleEntity implements AbstractTopic {

    private final SegmentedStorageTopic storageTopic;
    /**
     * Runtime produce state for this topic. Replicated to every pod's {@code TopicCache}; the
     * produce gate and topic failover read this field. Fence coordination uses {@link #getVersion()}
     * together with {@code TransitionEvent.topicVersionToAwait}.
     */
    private final TopicState topicState;
    /** When true, controller may automatically fail over this topic on region degradation. */
    private final boolean autoFailover;
    /** Per-region produce / standby policy; keyed by region name. */
    private final Map<String, RegionConfig> regionConfigs;
    private final boolean grouped;

    private final String nfrFilterName;
    private final TopicCategory topicCategory;

    /**
     * Override per topic. If producer config is disabled, this is ignored.
     */
    @Nullable
    private final RateLimiterMode rateLimiterMode;

    private final TopicCapacityPolicy capacity;
    private final Map<String, Double> perRegionQuotaWeights;

    // TODO: decide on where topic related auxiliary data lives. This data largely is not going to be used by crud, main produce or consume flow. But they power secondary functionalities.
    private final MessageSizeProfile messageSizeProfile;

    public enum TopicCategory {
        TOPIC, QUEUE
    }

    private VaradhiTopic(
        String name,
        int version,
        boolean grouped,
        TopicCapacityPolicy capacity,
        SegmentedStorageTopic storageTopic,
        TopicState topicState,
        boolean autoFailover,
        Map<String, RegionConfig> regionConfigs,
        LifecycleStatus status,
        String nfrFilterName,
        TopicCategory topicCategory,
        Map<String, Double> perRegionQuotaWeights,
        MessageSizeProfile messageSizeProfile,
        RateLimiterMode rateLimiterMode
    ) {
        super(name, version, MetaStoreEntityType.TOPIC);
        this.grouped = grouped;
        this.capacity = capacity;
        this.storageTopic = storageTopic;
        this.topicState = topicState;
        this.autoFailover = autoFailover;
        this.regionConfigs = new HashMap<>(regionConfigs);
        this.nfrFilterName = nfrFilterName;
        this.topicCategory = Objects.requireNonNull(topicCategory, "topicCategory must not be null");
        this.perRegionQuotaWeights = perRegionQuotaWeights != null ?
            new HashMap<>(perRegionQuotaWeights) :
            new HashMap<>();
        this.messageSizeProfile = messageSizeProfile;
        this.rateLimiterMode = rateLimiterMode;
        this.status = status;
    }

    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode
    ) {
        return of(project, name, grouped, capacity, actionCode, null);
    }

    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy
    ) {
        return of(project, name, grouped, capacity, actionCode, nfrStrategy, TopicCategory.TOPIC);
    }

    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy,
        TopicCategory topicCategory
    ) {
        return of(project, name, grouped, capacity, actionCode, nfrStrategy, topicCategory, null, null, null);
    }

    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy,
        TopicCategory topicCategory,
        Map<String, Double> perRegionQuotaWeights,
        MessageSizeProfile messageSizeProfile,
        RateLimiterMode rateLimiterMode
    ) {
        return new VaradhiTopic(
            fqn(project, name),
            INITIAL_VERSION,
            grouped,
            capacity,
            null,
            TopicState.Producing,
            false,
            new HashMap<>(),
            new LifecycleStatus(LifecycleStatus.State.CREATING, actionCode),
            nfrStrategy,
            topicCategory,
            perRegionQuotaWeights,
            messageSizeProfile,
            rateLimiterMode
        );
    }

    public static String fqn(String projectName, String topicName) {
        return VaradhiTopicName.of(projectName, topicName).toFqn();
    }

    /**
     * Sets {@link #storageTopic} on the first call and registers each {@code region} in
     * {@link #regionConfigs}. Additional regions share the same storage topic.
     */
    public VaradhiTopic addInternalTopic(String region, SegmentedStorageTopic segmentedTopic) {
        SegmentedStorageTopic resolvedStorage = storageTopic != null ? storageTopic : segmentedTopic;
        Map<String, RegionConfig> updatedConfigs = new HashMap<>(regionConfigs);
        boolean firstRegion = updatedConfigs.isEmpty();
        updatedConfigs.putIfAbsent(region, firstRegion ? RegionConfig.producing() : new RegionConfig(false, null));
        VaradhiTopic updated = new VaradhiTopic(
            getName(),
            getVersion(),
            grouped,
            capacity,
            resolvedStorage,
            topicState,
            autoFailover,
            updatedConfigs,
            getStatus(),
            nfrFilterName,
            topicCategory,
            perRegionQuotaWeights,
            messageSizeProfile,
            rateLimiterMode
        );
        updated.status = this.status;
        return updated;
    }

    @JsonIgnore
    public RegionConfig getRegionConfig(RegionName region) {
        return regionConfigs.get(region.value());
    }

    public VaradhiTopic withTopicState(TopicState state) {
        return copyWith(null, state, null);
    }

    public VaradhiTopic withAutoFailover(boolean autoFailover) {
        return copyWith(null, null, autoFailover);
    }

    @JsonIgnore
    public String getProjectName() {
        return VaradhiTopicName.parse(getName()).getProjectName();
    }

    @JsonIgnore
    public String getTopicName() {
        return VaradhiTopicName.parse(getName()).getTopicName();
    }

    public SegmentedStorageTopic getProduceTopicForRegion(String region) {
        return regionConfigs.containsKey(region) ? storageTopic : null;
    }

    public boolean isCategory(TopicCategory category) {
        return this.topicCategory == category;
    }

    VaradhiTopic copyWith(Map<String, RegionConfig> regionConfigs, TopicState topicState, Boolean autoFailover) {
        VaradhiTopic copy = new VaradhiTopic(
            getName(),
            getVersion(),
            grouped,
            capacity,
            storageTopic,
            topicState != null ? topicState : this.topicState,
            autoFailover != null ? autoFailover : this.autoFailover,
            regionConfigs != null ? regionConfigs : this.regionConfigs,
            getStatus(),
            nfrFilterName,
            topicCategory,
            perRegionQuotaWeights,
            messageSizeProfile,
            rateLimiterMode
        );
        copy.status = this.status;
        return copy;
    }
}

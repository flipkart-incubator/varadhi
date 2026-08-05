package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a topic in the Varadhi.
 *
 * <p>Per-region produce policy ({@link #produceConfigs}) is the single source of truth for
 * whether a region accepts produce ({@link TopicState}). Storage layout ({@link #segmentedStorageTopic})
 * is shared across regions — region membership in {@code produceConfigs} gates access, not a
 * per-region storage map.
 *
 * <p>Produce routing is resolved by {@link TopicResolver}, not on this entity.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class VaradhiTopic extends LifecycleEntity implements AbstractTopic {

    // --- Fields ---

    /** Shared storage segment for this topic; never null. */
    private final SegmentedStorageTopic segmentedStorageTopic;
    /** When true, controller may automatically fail over this topic on region degradation. */
    private final boolean autoFailover;
    /**
     * Per-region produce policy; keyed by {@link RegionName}. Serialized as {@code produceConfigs}
     * (wire shape: {@code state}, optional {@code failOverRegion}, {@code produceIdx}).
     */
    @JsonProperty ("produceConfigs")
    @Getter (lombok.AccessLevel.NONE)
    private final Map<RegionName, ProduceConfig> produceConfigs;
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

    // --- Construction ---

    /**
     * Constructs a new VaradhiTopic instance.
     *
     * @param name                  the fully-qualified topic name
     * @param version               the version of the topic
     * @param grouped               whether the topic is grouped
     * @param capacity              the capacity policy of the topic
     * @param segmentedStorageTopic shared segmented storage; must not be {@code null}
     * @param autoFailover          whether controller may auto-failover on region degradation
     * @param produceConfigs        per-region produce policy; keyed by {@link RegionName}; must not be {@code null}
     * @param status                the lifecycle status of the topic
     * @param nfrFilterName         the name of the filter applied for NFR; {@code null} if not set
     * @param topicCategory         topic vs queue classification; must not be {@code null}
     * @param perRegionQuotaWeights per-region fraction of global produce quota; nullable until defaulted
     * @param messageSizeProfile    observed message size profile; nullable until defaulted
     * @param rateLimiterMode       per-topic rate limiter rollout mode; nullable until defaulted
     */
    private VaradhiTopic(
        String name,
        int version,
        boolean grouped,
        TopicCapacityPolicy capacity,
        SegmentedStorageTopic segmentedStorageTopic,
        boolean autoFailover,
        Map<RegionName, ProduceConfig> produceConfigs,
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
        this.segmentedStorageTopic = Objects.requireNonNull(
            segmentedStorageTopic,
            "segmentedStorageTopic must not be null"
        );
        this.autoFailover = autoFailover;
        this.produceConfigs = new HashMap<>(Objects.requireNonNull(produceConfigs, "produceConfigs must not be null"));
        this.nfrFilterName = nfrFilterName;
        this.topicCategory = Objects.requireNonNull(topicCategory, "topicCategory must not be null");
        this.perRegionQuotaWeights = perRegionQuotaWeights != null ?
            new HashMap<>(perRegionQuotaWeights) :
            new HashMap<>();
        this.messageSizeProfile = messageSizeProfile;
        this.rateLimiterMode = rateLimiterMode;
        this.status = status;
    }

    // --- Factory methods ---

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
        RateLimiterMode rateLimiterMode,
        SegmentedStorageTopic segmentedStorageTopic,
        boolean autoFailover,
        Map<RegionName, ProduceConfig> produceConfigs
    ) {
        return new VaradhiTopic(
            fqn(project, name),
            INITIAL_VERSION,
            grouped,
            capacity,
            segmentedStorageTopic,
            autoFailover,
            produceConfigs,
            new LifecycleStatus(LifecycleStatus.State.CREATING, actionCode),
            nfrStrategy,
            topicCategory,
            perRegionQuotaWeights,
            messageSizeProfile,
            rateLimiterMode
        );
    }

    /**
     * Builds the topic name from the project name and topic name.
     *
     * @param projectName the name of the project
     * @param topicName   the name of the topic
     * @return the constructed topic name
     */
    public static String fqn(String projectName, String topicName) {
        return VaradhiTopicName.of(projectName, topicName).toFqn();
    }

    // --- Immutable updates ---

    /**
     * Sets the shared {@link #segmentedStorageTopic}. Replaces any previous value.
     */
    public VaradhiTopic with(SegmentedStorageTopic storageTopic) {
        return copyWith(storageTopic, produceConfigs, autoFailover);
    }

    /**
     * Sets shared storage and per-region produce policy in one copy.
     */
    public VaradhiTopic with(SegmentedStorageTopic storageTopic, RegionName region, ProduceConfig config) {
        Map<RegionName, ProduceConfig> updated = new HashMap<>(produceConfigs);
        updated.put(region, config);
        return copyWith(storageTopic, updated, autoFailover);
    }

    /**
     * Sets or registers per-region produce policy for {@code region}.
     * Does not change {@link #segmentedStorageTopic} — call {@link #with} separately.
     */
    public VaradhiTopic with(RegionName region, ProduceConfig config) {
        Map<RegionName, ProduceConfig> updated = new HashMap<>(produceConfigs);
        updated.put(region, config);
        return copyWith(updated, autoFailover);
    }

    // --- Read accessors ---

    @JsonIgnore
    public Optional<ProduceConfig> getProduceConfig(RegionName region) {
        return Optional.ofNullable(produceConfigs.get(region));
    }

    /**
     * Retrieves the project name from the topic name.
     *
     * @return the project name
     */
    @JsonIgnore
    public String getProjectName() {
        return VaradhiTopicName.parse(getName()).getProjectName();
    }

    /**
     * Local topic name (segment after the project prefix in the fully-qualified name).
     */
    @JsonIgnore
    public String getTopicName() {
        return VaradhiTopicName.parse(getName()).getTopicName();
    }

    /**
     * Whether this topic's {@link #getTopicCategory() category} equals {@code category}.
     *
     * @param category the category to compare against; must not be {@code null}
     * @return {@code true} if the topic's category equals {@code category}
     */
    public boolean isCategory(TopicCategory category) {
        return this.topicCategory == category;
    }

    // --- Copy helpers ---

    VaradhiTopic copyWith(Map<RegionName, ProduceConfig> produceConfigs, boolean autoFailover) {
        return copyWith(segmentedStorageTopic, produceConfigs, autoFailover);
    }

    private VaradhiTopic copyWith(
        SegmentedStorageTopic storageTopic,
        Map<RegionName, ProduceConfig> produceConfigs,
        boolean autoFailover
    ) {
        VaradhiTopic copy = new VaradhiTopic(
            getName(),
            getVersion(),
            grouped,
            capacity,
            storageTopic,
            autoFailover,
            produceConfigs,
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

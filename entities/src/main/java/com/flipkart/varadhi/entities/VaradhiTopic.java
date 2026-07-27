package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a topic in the Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class VaradhiTopic extends LifecycleEntity implements AbstractTopic {

    private final SegmentedStorageTopic storageTopic;
    /** When true, controller may automatically fail over this topic on region degradation. */
    private final boolean autoFailover;
    /**
     * Per-region produce policy; keyed by region. Multiple regions may be
     * {@link TopicState#Producing} on a global topic. Legacy JSON used {@code regionConfigs}.
     */
    @JsonProperty ("produceConfigs")
    @JsonAlias ("regionConfigs")
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

    /**
     * Full constructor used by {@link #of} and {@link #copyWith}. Private: all external construction
     * goes through the {@code of} factories or a {@code with*}/{@code copyWith} copy.
     */
    private VaradhiTopic(
        String name,
        int version,
        boolean grouped,
        TopicCapacityPolicy capacity,
        SegmentedStorageTopic storageTopic,
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
        this.storageTopic = storageTopic;
        this.autoFailover = autoFailover;
        this.produceConfigs = new HashMap<>(produceConfigs);
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

    public Map<RegionName, ProduceConfig> getProduceConfigs() {
        return Collections.unmodifiableMap(produceConfigs);
    }

    /**
     * Sets the shared {@link #storageTopic}. Replaces any previous value.
     */
    public VaradhiTopic withStorageTopic(SegmentedStorageTopic storageTopic) {
        return copyWith(storageTopic, produceConfigs, autoFailover);
    }

    /**
     * Registers {@code region} in {@link #produceConfigs}. First region starts
     * {@link TopicState#Producing}; further regions {@link TopicState#Blocked}.
     * Does not change {@link #storageTopic} — call {@link #withStorageTopic} separately.
     */
    public VaradhiTopic withProduceRegion(RegionName region) {
        Map<RegionName, ProduceConfig> updatedConfigs = new HashMap<>(produceConfigs);
        boolean firstRegion = updatedConfigs.isEmpty();
        updatedConfigs.putIfAbsent(region, firstRegion ? ProduceConfig.producing() : ProduceConfig.blocked());
        return copyWith(storageTopic, updatedConfigs, autoFailover);
    }

    @JsonIgnore
    public Optional<ProduceConfig> getProduceConfig(RegionName region) {
        return Optional.ofNullable(produceConfigs.get(region));
    }

    /**
     * Resolves the producer cache key for {@code region}: storage topic + produce region
     * ({@code failOverRegion} if set, otherwise {@code region}).
     *
     * <p>Does <em>not</em> check {@link TopicState#isProduceAllowed()} — use this when you need the
     * key for an already-cached producer (e.g. PREPARE participation while the region is
     * {@link TopicState#Fenced}). For the produce HTTP path use {@link #getProduceTopic}.
     */
    @JsonIgnore
    public Optional<ProduceTarget> resolveProduceTarget(String region) {
        return resolveProduceTarget(RegionName.of(region));
    }

    @JsonIgnore
    public Optional<ProduceTarget> resolveProduceTarget(RegionName region) {
        ProduceConfig config = produceConfigs.get(region);
        if (config == null || storageTopic == null) {
            return Optional.empty();
        }
        RegionName produceRegion = config.failOverRegion() != null ? config.failOverRegion() : region;
        if (!produceConfigs.containsKey(produceRegion)) {
            return Optional.empty();
        }
        return Optional.of(new ProduceTarget(storageTopic.getTopicToProduce(), produceRegion));
    }

    /**
     * Resolves where produce for {@code region} should go.
     *
     * <p>Empty when this region has no produce config, produce is not allowed ({@link TopicState}),
     * or storage is not provisioned. When present, {@link ProduceTarget#produceRegion()} is
     * {@code failOverRegion} if set, otherwise {@code region}.
     */
    @JsonIgnore
    public Optional<ProduceTarget> getProduceTopic(String region) {
        return getProduceTopic(RegionName.of(region));
    }

    @JsonIgnore
    public Optional<ProduceTarget> getProduceTopic(RegionName region) {
        ProduceConfig config = produceConfigs.get(region);
        if (config == null || !config.state().isProduceAllowed()) {
            return Optional.empty();
        }
        return resolveProduceTarget(region);
    }

    /**
     * Returns a copy with {@code region}'s {@link ProduceConfig} replaced; all other regions unchanged.
     */
    public VaradhiTopic withProduceConfig(RegionName region, ProduceConfig config) {
        Map<RegionName, ProduceConfig> updated = new HashMap<>(produceConfigs);
        updated.put(region, config);
        return copyWith(updated, autoFailover);
    }

    /**
     * Returns a copy with {@link #autoFailover} set to the given value.
     */
    public VaradhiTopic withAutoFailover(boolean autoFailover) {
        return copyWith(produceConfigs, autoFailover);
    }

    // Note: Lombok @With is not used for these copy methods because fields are inherited from
    // MetaStoreEntity / LifecycleEntity, which @With cannot see (same reason as Region.java).

    @JsonIgnore
    public String getProjectName() {
        return VaradhiTopicName.parse(getName()).getProjectName();
    }

    @JsonIgnore
    public String getTopicName() {
        return VaradhiTopicName.parse(getName()).getTopicName();
    }

    /**
     * Resolves the shared {@link #storageTopic} for {@code region} regardless of {@link TopicState}.
     * {@code null} if {@code region} has no produce config or storage is not yet provisioned.
     */
    public SegmentedStorageTopic getProduceTopicForRegion(String region) {
        return getProduceTopicForRegion(RegionName.of(region));
    }

    /**
     * @see #getProduceTopicForRegion(String)
     */
    public SegmentedStorageTopic getProduceTopicForRegion(RegionName region) {
        return produceConfigs.containsKey(region) ? storageTopic : null;
    }

    public boolean isCategory(TopicCategory category) {
        return this.topicCategory == category;
    }

    /**
     * Package-visible copy helper used by other entities' {@code with*} builders that only need to
     * touch {@link #produceConfigs} / {@link #autoFailover}; delegates to the private full copy.
     */
    VaradhiTopic copyWith(Map<RegionName, ProduceConfig> produceConfigs, boolean autoFailover) {
        return copyWith(storageTopic, produceConfigs, autoFailover);
    }

    /**
     * Private copy helper backing all {@code with*} methods: builds a fresh instance carrying over
     * every field, substituting the given ones. No null sentinels — callers must pass full replacement
     * values (e.g. the current {@link #produceConfigs} unchanged) rather than relying on null-means-keep.
     */
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

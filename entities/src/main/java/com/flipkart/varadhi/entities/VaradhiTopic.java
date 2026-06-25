package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    private final Map<String, SegmentedStorageTopic> internalTopics;
    /**
     * Runtime produce state per region (keyed by the same region key used in
     * {@link #internalTopics}). Moved here from {@code SegmentedStorageTopic} so the
     * topic — the entity replicated to every pod's {@code TopicCache} — owns the
     * routing-relevant state that the produce gate and topic failover read.
     */
    private final Map<String, TopicState> topicStates;
    private final boolean grouped;
    private final TopicCapacityPolicy capacity;
    private final String nfrFilterName;
    private final TopicCategory topicCategory;

    public enum TopicCategory {
        TOPIC, QUEUE
    }

    /**
     * Constructs a new VaradhiTopic instance.
     *
     * @param name           the name of the topic
     * @param version        the version of the topic
     * @param grouped        whether the topic is grouped
     * @param capacity       the capacity policy of the topic
     * @param internalTopics the internal topics associated with this topic
     * @param status         the status of the topic
     * @param nfrFilterName  the name of the filter applied for NFR; {@code null} if not set
     * @param topicCategory  topic vs queue classification; must not be {@code null}
     */
    private VaradhiTopic(
        String name,
        int version,
        boolean grouped,
        TopicCapacityPolicy capacity,
        Map<String, SegmentedStorageTopic> internalTopics,
        Map<String, TopicState> topicStates,
        LifecycleStatus status,
        String nfrFilterName,
        TopicCategory topicCategory
    ) {
        super(name, version, MetaStoreEntityType.TOPIC);
        this.grouped = grouped;
        this.capacity = capacity;
        this.internalTopics = internalTopics;
        this.topicStates = topicStates != null ? topicStates : new HashMap<>();
        this.nfrFilterName = nfrFilterName;
        this.topicCategory = Objects.requireNonNull(topicCategory, "topicCategory must not be null");
        this.status = status;
    }

    /**
     * Creates a new VaradhiTopic instance.
     *
     * @param project   the project associated with the topic
     * @param name      the name of the topic
     * @param grouped   whether the topic is grouped
     * @param capacity  the capacity policy of the topic
     * @param actionCode the actor code indicating the reason for the state
     * @return a new VaradhiTopic instance
     */
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

    /**
     * Same as {@link #of(String, String, boolean, TopicCapacityPolicy, LifecycleStatus.ActionCode, String)} but
     * sets {@link TopicCategory} (e.g. {@link TopicCategory#QUEUE} for the topic leg of a queue).
     */
    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy,
        TopicCategory topicCategory
    ) {
        return new VaradhiTopic(
            fqn(project, name),
            INITIAL_VERSION,
            grouped,
            capacity,
            new HashMap<>(),
            new HashMap<>(),
            new LifecycleStatus(LifecycleStatus.State.CREATING, actionCode),
            nfrStrategy,
            topicCategory
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

    /**
     * Adds an internal topic for a specific region and initializes the region's produce
     * state to {@link TopicState#Producing} if not already set.
     *
     * @param region        the region for the internal topic
     * @param internalTopic the internal topic to add
     */
    public void addInternalTopic(String region, SegmentedStorageTopic internalTopic) {
        this.internalTopics.put(region, internalTopic);
        this.topicStates.putIfAbsent(region, TopicState.Producing);
    }

    /**
     * Returns the produce {@link TopicState} for {@code region}, defaulting to
     * {@link TopicState#Producing} when no explicit state has been recorded for a region
     * that has an internal topic.
     *
     * @param region the region whose produce state is requested
     * @return the region's produce state, or {@code null} if the region is unknown to this topic
     */
    public TopicState getTopicState(RegionName region) {
        Objects.requireNonNull(region, "region must not be null");
        String key = region.value();
        if (!internalTopics.containsKey(key)) {
            return null;
        }
        return topicStates.getOrDefault(key, TopicState.Producing);
    }

    /**
     * Sets the produce {@link TopicState} for {@code region}. Used by topic failover to flip
     * a region between {@link TopicState#Producing} and {@link TopicState#Replicating}.
     *
     * @param region the region whose produce state is being set
     * @param state  the new produce state; must not be {@code null}
     */
    public void setTopicState(RegionName region, TopicState state) {
        Objects.requireNonNull(region, "region must not be null");
        Objects.requireNonNull(state, "topic state must not be null");
        this.topicStates.put(region.value(), state);
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
     * Retrieves the produce topic for a specific region.
     *
     * @param region the region for which to retrieve the produce topic
     * @return the produce topic for the specified region
     */
    public SegmentedStorageTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
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
}

package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a topic in the Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class VaradhiTopic extends LifecycleEntity implements AbstractTopic {

    private final Map<String, InternalCompositeTopic> internalTopics;
    private final boolean grouped;
    private final TopicCapacityPolicy capacity;
    //Should it be list<String> ?
    private final String nfrStrategy;

    /**
     * Constructs a new VaradhiTopic instance.
     *
     * @param name           the name of the topic
     * @param version        the version of the topic
     * @param grouped        whether the topic is grouped
     * @param capacity       the capacity policy of the topic
     * @param internalTopics the internal topics associated with this topic
     * @param status         the status of the topic
     * @param nfrStrategy    the optional nfrStrategy
     */
    private VaradhiTopic(
        String name,
        int version,
        boolean grouped,
        TopicCapacityPolicy capacity,
        Map<String, InternalCompositeTopic> internalTopics,
        LifecycleStatus status,
        String nfrStrategy
    ) {
        super(name, version);
        this.grouped = grouped;
        this.capacity = capacity;
        this.internalTopics = internalTopics;
        this.nfrStrategy = nfrStrategy;
        this.status = status;
    }

    /**
     * Creates a new VaradhiTopic instance.
     *
     * @param project   the project associated with the topic
     * @param name      the name of the topic
     * @param grouped   whether the topic is grouped
     * @param capacity  the capacity policy of the topic
     * @param actorCode the actor code indicating the reason for the state
     * @return a new VaradhiTopic instance
     */
    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActorCode actorCode
    ) {
        return of(project, name, grouped, capacity, actorCode, null);
    }

    public static VaradhiTopic of(
        String project,
        String name,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActorCode actorCode,
        String nfrStrategy
    ) {
        return new VaradhiTopic(
            buildTopicName(project, name),
            INITIAL_VERSION,
            grouped,
            capacity,
            new HashMap<>(),
            new LifecycleStatus(LifecycleStatus.State.CREATING, actorCode),
            nfrStrategy
        );
    }

    /**
     * Builds the topic name from the project name and topic name.
     *
     * @param projectName the name of the project
     * @param topicName   the name of the topic
     * @return the constructed topic name
     */
    public static String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }

    /**
     * Adds an internal topic for a specific region.
     *
     * @param region        the region for the internal topic
     * @param internalTopic the internal topic to add
     */
    public void addInternalTopic(String region, InternalCompositeTopic internalTopic) {
        this.internalTopics.put(region, internalTopic);
    }

    /**
     * Retrieves the project name from the topic name.
     *
     * @return the project name
     */
    @JsonIgnore
    public String getProjectName() {
        return getName().split(NAME_SEPARATOR_REGEX)[0];
    }

    /**
     * Retrieves the produce topic for a specific region.
     *
     * @param region the region for which to retrieve the produce topic
     * @return the produce topic for the specified region
     */
    public InternalCompositeTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
    }
}

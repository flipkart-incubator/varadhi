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
@EqualsAndHashCode(callSuper = true)
public class VaradhiTopic extends AbstractTopic {

    private final Map<String, InternalCompositeTopic> internalTopics;
    private final boolean grouped;
    private final TopicCapacityPolicy capacity;
    private final LifecycleStatus status;

    /**
     * Constructs a new VaradhiTopic instance.
     *
     * @param name           the name of the topic
     * @param version        the version of the topic
     * @param grouped        whether the topic is grouped
     * @param capacity       the capacity policy of the topic
     * @param internalTopics the internal topics associated with this topic
     * @param status         the status of the topic
     */
    private VaradhiTopic(
            String name, int version, boolean grouped, TopicCapacityPolicy capacity,
            Map<String, InternalCompositeTopic> internalTopics, LifecycleStatus status
    ) {
        super(name, version);
        this.grouped = grouped;
        this.capacity = capacity;
        this.internalTopics = internalTopics;
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
     *
     * @return a new VaradhiTopic instance
     */
    public static VaradhiTopic of(
            String project, String name, boolean grouped, TopicCapacityPolicy capacity,
            LifecycleStatus.ActorCode actorCode
    ) {
        return new VaradhiTopic(
                buildTopicName(project, name), INITIAL_VERSION, grouped, capacity, new HashMap<>(),
                new LifecycleStatus(LifecycleStatus.State.CREATING, actorCode)
        );
    }

    /**
     * Builds the topic name from the project name and topic name.
     *
     * @param projectName the name of the project
     * @param topicName   the name of the topic
     *
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
     *
     * @return the produce topic for the specified region
     */
    public InternalCompositeTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
    }

    /**
     * Marks the topic as created.
     */
    public void markCreated() {
        this.status.update(LifecycleStatus.State.CREATED);
    }

    /**
     * Marks the topic as created with the specified actor code and message.
     *
     * @param actorCode the actor code indicating the reason for the state
     * @param message   the message associated with the state
     */
    public void markCreated(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.CREATED, message, actorCode);
    }

    /**
     * Marks the topic creation as failed with the specified message.
     *
     * @param message the message associated with the failure
     */
    public void markCreateFailed(String message) {
        this.status.update(LifecycleStatus.State.CREATE_FAILED, message);
    }

    /**
     * Marks the topic as deleting with the specified actor code and message.
     *
     * @param actorCode the actor code indicating the reason for the state
     * @param message   the message associated with the state
     */
    public void markDeleting(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.DELETING, message, actorCode);
    }

    /**
     * Marks the topic deletion as failed with the specified message.
     *
     * @param message the message associated with the failure
     */
    public void markDeleteFailed(String message) {
        this.status.update(LifecycleStatus.State.DELETE_FAILED, message);
    }

    /**
     * Marks the topic as inactive.
     *
     * @param actorCode the actor code indicating why the topic is being marked as inactive
     * @param message   the message for the action
     */
    public void markInactive(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.INACTIVE, message, actorCode);
    }

    /**
     * Checks if the topic is active.
     *
     * @return true if the topic is in CREATED state, false otherwise
     */
    @JsonIgnore
    public boolean isActive() {
        return this.status.getState() == LifecycleStatus.State.CREATED;
    }

    /**
     * Checks if the topic's current state is retriable.
     * A state is considered retriable if it is either CREATE_FAILED or DELETE_FAILED.
     *
     * @return true if the topic's state is retriable, false otherwise
     */
    @JsonIgnore
    public boolean isRetriable() {
        return this.status.getState().isRetriable();
    }
}

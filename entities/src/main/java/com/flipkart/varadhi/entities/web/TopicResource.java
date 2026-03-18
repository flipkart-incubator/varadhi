package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.ValidateResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;


/**
 * Represents a topic resource in the Varadhi system (API request/response and factory input).
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Topic name. Check naming constraints.", max = 64)
public class TopicResource extends BaseResource implements Validatable {

    @Setter
    private TopicCapacityPolicy capacity;

    @Setter
    private LifecycleStatus.ActionCode actionCode;
    private final String nfrFilterName;

    /**
     * Constructs a new TopicResource instance.
     */
    private TopicResource(
        String name,
        int version,
        String project,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrFilterName
    ) {
        super(name, version);
        setProject(project);
        setGrouped(grouped);
        this.capacity = capacity;
        this.actionCode = actionCode;
        this.nfrFilterName = nfrFilterName;
    }

    /** Whether the topic is grouped; uses base resource grouped flag. */
    public boolean isGrouped() {
        return Boolean.TRUE.equals(getGrouped());
    }

    /**
     * Creates a new grouped TopicResource instance.
     */
    public static TopicResource grouped(
        String name,
        String project,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy
    ) {
        return new TopicResource(name, INITIAL_VERSION, project, true, capacity, actionCode, nfrStrategy);
    }

    /**
     * Creates a new ungrouped TopicResource instance.
     */
    public static TopicResource unGrouped(
        String name,
        String project,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrStrategy
    ) {
        return new TopicResource(name, INITIAL_VERSION, project, false, capacity, actionCode, nfrStrategy);
    }

    /**
     * Creates a TopicResource instance from a VaradhiTopic instance.
     */
    public static TopicResource from(VaradhiTopic varadhiTopic) {
        String[] topicResourceInfo = varadhiTopic.getName().split(NAME_SEPARATOR_REGEX);

        return new TopicResource(
            topicResourceInfo[1],
            varadhiTopic.getVersion(),
            topicResourceInfo[0],
            varadhiTopic.isGrouped(),
            varadhiTopic.getCapacity(),
            varadhiTopic.getStatus().getActionCode(),
            varadhiTopic.getNfrFilterName()
        );
    }

    /**
     * Converts this TopicResource instance to a VaradhiTopic instance.
     */
    public VaradhiTopic toVaradhiTopic() {
        return VaradhiTopic.of(getProject(), getName(), isGrouped(), capacity, actionCode, nfrFilterName);
    }
}

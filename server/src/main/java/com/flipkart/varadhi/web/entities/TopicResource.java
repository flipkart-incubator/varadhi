package com.flipkart.varadhi.web.entities;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.ValidateResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.Versioned;
import jakarta.validation.constraints.NotBlank;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;


/**
 * Represents a topic resource in the Varadhi system.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Topic name. Check naming constraints.", max = 64)
public class TopicResource extends Versioned implements Validatable {

    @NotBlank
    private final String project;

    private final boolean grouped;

    @Setter
    private TopicCapacityPolicy capacity;

    @Setter
    private LifecycleStatus.ActorCode actorCode;
    private final String nfrFilterName;

    /**
     * Constructs a new TopicResource instance.
     *
     * @param name      the name of the topic
     * @param version   the version of the topic
     * @param project   the project associated with the topic
     * @param grouped   whether the topic is grouped
     * @param capacity  the capacity policy of the topic
     * @param actorCode the actor code indicating reason behind the action performed on the topic
     */
    private TopicResource(
        String name,
        int version,
        String project,
        boolean grouped,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActorCode actorCode,
        String nfrFilterName
    ) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.capacity = capacity;
        this.actorCode = actorCode;
        this.nfrFilterName = nfrFilterName;
    }

    /**
     * Creates a new grouped TopicResource instance.
     *
     * @param name      the name of the topic
     * @param project   the project associated with the topic
     * @param capacity  the capacity policy of the topic
     * @param actorCode the actor code indicating reason behind the action performed on the topic
     *
     * @return a new grouped TopicResource instance
     */
    public static TopicResource grouped(
        String name,
        String project,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActorCode actorCode,
        String nfrStrategy
    ) {
        return new TopicResource(name, INITIAL_VERSION, project, true, capacity, actorCode, nfrStrategy);
    }

    /**
     * Creates a new ungrouped TopicResource instance.
     *
     * @param name      the name of the topic
     * @param project   the project associated with the topic
     * @param capacity  the capacity policy of the topic
     * @param actorCode the actor code indicating reason behind the action performed on the topic
     *
     * @return a new ungrouped TopicResource instance
     */
    public static TopicResource unGrouped(
        String name,
        String project,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActorCode actorCode,
        String nfrStrategy
    ) {
        return new TopicResource(name, INITIAL_VERSION, project, false, capacity, actorCode, nfrStrategy);
    }

    /**
     * Creates a TopicResource instance from a VaradhiTopic instance.
     *
     * @param varadhiTopic the VaradhiTopic instance
     *
     * @return a new TopicResource instance
     */
    public static TopicResource from(VaradhiTopic varadhiTopic) {
        String[] topicResourceInfo = varadhiTopic.getName().split(NAME_SEPARATOR_REGEX);

        return new TopicResource(
            topicResourceInfo[1],
            varadhiTopic.getVersion(),
            topicResourceInfo[0],
            varadhiTopic.isGrouped(),
            varadhiTopic.getCapacity(),
            varadhiTopic.getStatus().getActorCode(),
            varadhiTopic.getNfrFilterName()
        );
    }

    /**
     * Converts this TopicResource instance to a VaradhiTopic instance.
     *
     * @return a new VaradhiTopic instance
     */
    public VaradhiTopic toVaradhiTopic() {
        return VaradhiTopic.of(project, getName(), grouped, capacity, actorCode, nfrFilterName);
    }
}

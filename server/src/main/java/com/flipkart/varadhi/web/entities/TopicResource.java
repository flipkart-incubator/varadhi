package com.flipkart.varadhi.web.entities;

import com.flipkart.varadhi.entities.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateResource(message = "Invalid Topic name. Check naming constraints.", max = 64)
public class TopicResource extends VersionedEntity implements Validatable {
    private static final String RESOURCE_TYPE_NAME = "TopicResource";
    private final String project;
    private final boolean grouped;
    @Setter
    private TopicCapacityPolicy capacity;

    private TopicResource(
            String name,
            int version,
            String project,
            boolean grouped,
            TopicCapacityPolicy capacity
    ) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.capacity = capacity;
    }

    public static TopicResource grouped(String name, String project, TopicCapacityPolicy capacity) {
        return new TopicResource(name, INITIAL_VERSION, project, true, capacity);
    }

    public static TopicResource unGrouped(String name, String project, TopicCapacityPolicy capacity) {
        return new TopicResource(name, INITIAL_VERSION, project, false, capacity);
    }

    public static TopicResource from(VaradhiTopic varadhiTopic) {
        String[] topicResourceInfo = varadhiTopic.getName().split(NAME_SEPARATOR_REGEX);
        return new TopicResource(
                topicResourceInfo[1],
                varadhiTopic.getVersion(),
                topicResourceInfo[0],
                varadhiTopic.isGrouped(),
                varadhiTopic.getCapacity()
        );
    }

    public VaradhiTopic toVaradhiTopic() {
        return VaradhiTopic.of(getProject(), getName(), isGrouped(), getCapacity());
    }
}

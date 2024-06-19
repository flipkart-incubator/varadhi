package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
@EqualsAndHashCode(callSuper = true)
public class VaradhiTopic extends AbstractTopic {

    private final Map<String, InternalCompositeTopic> internalTopics;
    private final boolean grouped;
    private final TopicCapacityPolicy capacity;

    private VaradhiTopic(
            String name,
            int version,
            boolean grouped,
            TopicCapacityPolicy capacity,
            Map<String, InternalCompositeTopic> internalTopics
    ) {
        super(name, version);
        this.grouped = grouped;
        this.capacity = capacity;
        this.internalTopics = internalTopics;
    }

    public static VaradhiTopic of(TopicResource topicResource) {
        return new VaradhiTopic(
                buildTopicName(topicResource.getProject(), topicResource.getName()),
                INITIAL_VERSION,
                topicResource.isGrouped(),
                topicResource.getCapacity(),
                new HashMap<>()
        );
    }

    public static String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }

    public void addInternalTopic(String region, InternalCompositeTopic internalTopic) {
        this.internalTopics.put(region, internalTopic);
    }

    @JsonIgnore
    public String getProjectName() {
        return getName().split(NAME_SEPARATOR_REGEX)[0];
    }

    public InternalCompositeTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
    }
}

package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class VaradhiTopic extends AbstractTopic {


    private final Map<String, InternalTopic> internalTopics;
    private final boolean grouped;

    private VaradhiTopic(
            String name,
            int version,
            boolean grouped,
            Map<String, InternalTopic> internalTopics
    ) {
        super(name, version);
        this.grouped = grouped;
        this.internalTopics = null == internalTopics ? new HashMap<>() : internalTopics;
    }

    public static VaradhiTopic of(TopicResource topicResource) {
        return new VaradhiTopic(
                buildTopicName(topicResource.getProject(), topicResource.getName()),
                INITIAL_VERSION,
                topicResource.isGrouped(),
                null
        );
    }

    public static String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }

    public void addInternalTopic(InternalTopic internalTopic) {
        this.internalTopics.put(internalTopic.getTopicRegion(), internalTopic);
    }

    public InternalTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
    }

    public TopicResource getTopicResource(String projectName) {
        return new TopicResource(
                this.getName().split("\\.")[1],
                this.getVersion(),
                projectName,
                this.isGrouped(),
                CapacityPolicy.getDefault()
        );
    }
}

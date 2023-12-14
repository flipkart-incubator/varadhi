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

    /**
     * TODO: evaluate whether it should return Optional or not. An unmapped region probably wouldn't be queried.
     *
     * @param region
     *
     * @return InternalTopic for the given region
     */
    public InternalTopic getProduceTopicForRegion(String region) {
        return internalTopics.get(region);
    }
}

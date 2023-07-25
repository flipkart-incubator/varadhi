package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class VaradhiTopic extends BaseTopic {
    private final Map<String, InternalTopic> internalTopics;
    private final boolean grouped;

    public VaradhiTopic(
            String name,
            int version,
            boolean grouped,
            Map<String, InternalTopic> internalTopics
    ) {
        super(name, version);
        this.grouped = grouped;
        this.internalTopics = null == internalTopics ? new ConcurrentHashMap<>() : internalTopics;
    }

    public static String getTopicFQN(String projectName, String topicName) {
        return String.format("%s.%s", projectName, topicName);
    }

    public void addInternalTopic(InternalTopic internalTopic) {
        this.internalTopics.put(internalTopic.getName(), internalTopic);
    }

    public InternalTopic getInternalMainTopic(String varadhiTopicName, String region) {
        String regionMainTopicName = InternalTopic.internalMainTopicName(varadhiTopicName, region);
        InternalTopic internalTopic = this.internalTopics.get(regionMainTopicName);
        if (null == internalTopic) {
            throw new ResourceNotFoundException(
                    String.format("Storage Topic %s of kind(%s) not found.", varadhiTopicName,
                            InternalTopic.TopicKind.Main
                    ));
        }
        return internalTopic;
    }

}

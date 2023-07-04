package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class VaradhiTopic extends BaseTopic {
    private static final String RESOURCE_TYPE_NAME = "VaradhiTopic";
    private final Map<InternalTopic.TopicKind, InternalTopic> internalTopics;
    private final boolean isGrouped;
    private final boolean isExclusiveSubscription;

    public VaradhiTopic(
            String name,
            int version,
            boolean isGrouped,
            boolean isExclusiveSubscription,
            Map<InternalTopic.TopicKind, InternalTopic> internalTopics
    ) {
        super(name, version);
        this.isGrouped = isGrouped;
        this.isExclusiveSubscription = isExclusiveSubscription;
        this.internalTopics = null == internalTopics ? new ConcurrentHashMap<>() : internalTopics;
    }


    public void addInternalTopic(InternalTopic internalTopic) {
        internalTopics.put(internalTopic.getTopicKind(), internalTopic);
    }

}

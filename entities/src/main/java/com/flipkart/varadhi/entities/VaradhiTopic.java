package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class VaradhiTopic extends BaseTopic {
    private final Map<InternalTopic.TopicKind, InternalTopic> internalTopics;
    private final boolean grouped;
    private final boolean exclusiveSubscription;

    public VaradhiTopic(
            String name,
            int version,
            boolean grouped,
            boolean exclusiveSubscription,
            Map<InternalTopic.TopicKind, InternalTopic> internalTopics
    ) {
        super(name, version);
        this.grouped = grouped;
        this.exclusiveSubscription = exclusiveSubscription;
        this.internalTopics = null == internalTopics ? new ConcurrentHashMap<>() : internalTopics;
    }


    public void addInternalTopic(InternalTopic internalTopic) {
        internalTopics.put(internalTopic.getTopicKind(), internalTopic);
    }

}

package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class VaradhiTopic extends  BaseTopic {
    private final Map<InternalTopic.TopicKind, InternalTopic> internalTopics;
    private final boolean isGrouped;
    private final boolean isExclusiveSubscription;

    public VaradhiTopic(String name, boolean isGrouped, boolean isExclusiveSubscription) {
        super(name, InternalTopic.StorageKind.Meta);
        this.isGrouped = isGrouped;
        this.isExclusiveSubscription = isExclusiveSubscription;
        this.internalTopics = new ConcurrentHashMap<>();
    }
}

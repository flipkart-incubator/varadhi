package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class InternalTopic {

    private final String name;
    private final String topicRegion;
    private final TopicState topicState;
    private final StorageTopic storageTopic;
}

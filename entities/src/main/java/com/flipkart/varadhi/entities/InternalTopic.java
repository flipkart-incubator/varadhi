package com.flipkart.varadhi.entities;


import lombok.Data;

/**
 * A wrapper on the storage topic. In future this class will handle the adding additional storage topics for the purpose
 * of increasing partition count without affecting ordering.
 *
 * InternalTopic == CompositeTopic.
 */
@Data
public class InternalTopic {

    private final String topicRegion;
    private final TopicState topicState;
    private final StorageTopic storageTopic;

    public String getName() {
        return storageTopic.getName();
    }
}

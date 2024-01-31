package com.flipkart.varadhi.entities;


import lombok.Data;

/**
 * A wrapper on the storage topic. In future this class will handle the adding additional storage topics for the purpose
 * of increasing partition count without affecting ordering.
 * This concept is internal and is never exposed to the user.
 */
@Data
public class InternalCompositeTopic {

    private final String topicRegion;

    private final TopicState topicState;

    /**
     * As of now only 1 is supported, but in future this can be an array where we can add more storage topics.
     */
    private final StorageTopic storageTopic;
}

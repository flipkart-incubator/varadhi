package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class InternalTopic {

    private String topicRegion;
    private TopicState topicState;
    private String name;
    private StorageTopic storageTopic;

    public InternalTopic(
            String name,
            String topicRegion,
            TopicState topicState,
            StorageTopic storageTopic
    ) {
        this.name = name;
        this.topicRegion = topicRegion;
        this.topicState = topicState;
        this.storageTopic = storageTopic;
    }


    public enum TopicState {
        // topic allows produce, default status.
        Producing,
        // topic produce is blocked (e.g. administrative reasons)
        Blocked,
        // topic is being throttled currently.
        Throttled,
        // produce not allowed to passive replicated topic.
        Replicating
    }

    public boolean produceAllowed() {
        return topicState == TopicState.Producing;
    }

}

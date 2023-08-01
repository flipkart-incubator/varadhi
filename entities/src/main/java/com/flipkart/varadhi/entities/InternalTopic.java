package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class InternalTopic {

    private String topicRegion;
    private TopicKind topicKind;
    private TopicStatus topicStatus;
    private String name;
    private StorageTopic storageTopic;

    //TODO::check if reference to primary topic also needs to be kept.
    private String replicatingFromRegion; // Region this topic will be replicating from, in case it is replica topic.

    public InternalTopic(
            String name,
            TopicKind topicKind,
            String topicRegion,
            String replicatingFromRegion,
            TopicStatus topicStatus,
            StorageTopic storageTopic
    ) {
        this.name = name;
        this.topicKind = topicKind;
        this.topicRegion = topicRegion;
        this.topicStatus = topicStatus;
        this.replicatingFromRegion = replicatingFromRegion;
        this.storageTopic = storageTopic;
    }

    //internal Topic FQDN format is "<varadhiTopicName>.<kind>.<topicRegion>[.<replicatingFromRegion>]
    //should be unique w.r.to <varadhiTopicName>
    public static String internalMainTopicName(String varadhiTopicName, String region) {
        return String.format("%s.%s.%s", varadhiTopicName, InternalTopic.TopicKind.Main, region);
    }

    public static String internalReplicaTopicName(
            String varadhiTopicName,
            String replicaRegion,
            String replicatingFromRegion
    ) {
        return String.format(
                "%s.%s.%s.%s", varadhiTopicName, InternalTopic.TopicKind.Replica, replicaRegion, replicatingFromRegion);
    }

    public enum TopicKind {
        Main,
        Replica
    }

    public enum TopicStatus {
        // topic allows produce, default status.
        Active,
        // topic produce is blocked (e.g. administrative reasons)
        Blocked,
        // topic is being throttled currently.
        Throttled,
        // produce not allowed to passive replicated topic.
        NotAllowed
    }

}

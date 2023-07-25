package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class InternalTopic {

    private String topicRegion;
    private TopicKind topicKind;
    private ProduceStatus status;
    private String name;
    private StorageTopic storageTopic;

    //TODO::check if reference to primary topic also needs to be kept.
    private String replicatingFromRegion; // Region this topic will be replicating from, in case it is replica topic.

    private InternalTopic(
            String name,
            TopicKind topicKind,
            String topicRegion,
            String replicatingFromRegion,
            ProduceStatus status,
            StorageTopic storageTopic
    ) {
        this.name = name;
        this.topicKind = topicKind;
        this.topicRegion = topicRegion;
        this.status = status;
        this.replicatingFromRegion = replicatingFromRegion;
        this.storageTopic = storageTopic;
    }

    public static InternalTopic mainTopicFrom(
            Project project,
            String varadhiTopicName,
            String topicRegion,
            TopicResource topicResource,
            StorageTopicFactory<StorageTopic> topicFactory
    ) {
        String internalTopicName = internalMainTopicName(varadhiTopicName, topicRegion);
        StorageTopic storageTopic =
                topicFactory.getTopic(project, internalTopicName, topicResource.getCapacityPolicy());
        return new InternalTopic(
                internalTopicName,
                TopicKind.Main,
                topicRegion,
                null,
                ProduceStatus.Active,
                storageTopic
        );
    }

    //internal Topic FQDN format is "<varadhiTopicName>.<kind>.<topicRegion>[.<replicatingFromRegion>]
    //should be unique w.r.to <varadhiTopicName>
    public static String internalMainTopicName(String varadhiTopicName, String region) {
        return String.format("%s.%s.%s", varadhiTopicName, TopicKind.Main, region);
    }

    public static String internalReplicaTopicName(
            String varadhiTopicName,
            String replicaRegion,
            String replicatingFromRegion
    ) {
        return String.format("%s.%s.%s.%s", varadhiTopicName, TopicKind.Replica, replicaRegion, replicatingFromRegion);
    }

    public enum TopicKind {
        Main,
        Replica
    }

    public enum ProduceStatus {
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

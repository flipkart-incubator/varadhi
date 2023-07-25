package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class InternalTopic {

    private String region;
    private TopicKind topicKind;
    private ProduceStatus status;
    private String name;
    private StorageTopic storageTopic;

    //TODO::check if reference to primary topic also needs to be kept.
    private String sourceRegion; //zone this topic will be replicating from, in case it is replica topic.

    private InternalTopic(
            String name,
            TopicKind topicKind,
            String region,
            String sourceRegion,
            ProduceStatus status,
            StorageTopic storageTopic
    ) {
        this.name = name;
        this.topicKind = topicKind;
        this.region = region;
        this.status = status;
        this.sourceRegion = sourceRegion;
        this.storageTopic = storageTopic;
    }

    public static InternalTopic from(
            Project project,
            String varadhiTopicName,
            TopicResource topicResource,
            StorageTopicFactory<StorageTopic> topicFactory
    ) {
        String region = "local";

        //TODO::fix -- internalTopicName and InternalTopic are disconnected w.r.to TopicKind.

        String internalTopicName = internalTopicName(varadhiTopicName, region, null);
        StorageTopic storageTopic =
                topicFactory.getTopic(project, internalTopicName, topicResource.getCapacityPolicy());
        return new InternalTopic(
                internalTopicName,
                TopicKind.Main,
                "local",
                null,
                ProduceStatus.Active,
                storageTopic
        );
    }

    public static String internalTopicName(
            String varadhiTopicName, String region, String sourceRegion
    ) {
        //internal Topic FQDN format is "<varadhiTopicName>.<kind>.<region>[.<sourceRegion>]
        //should be unique w.r.to <varadhiTopicName>
        return null == sourceRegion ? internalMainTopicName(varadhiTopicName, region) :
                internalReplicaTopicName(varadhiTopicName, region, sourceRegion);
    }

    public static String internalMainTopicName(String varadhiTopicName, String region) {
        return String.format("%s.%s.%s", varadhiTopicName, TopicKind.Main, region);
    }

    public static String internalReplicaTopicName(String varadhiTopicName, String replicaRegion, String sourceRegion) {
        return String.format("%s.%s.%s.%s", varadhiTopicName, TopicKind.Replica, replicaRegion, sourceRegion);
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

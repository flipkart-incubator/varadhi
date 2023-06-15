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

    //TODO::check if private ctor suffices for Jackson.
    //also see if lombok allargsconstructor with accessiblity  modifier would work as well.
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
            String varadhiTopicName, TopicResource topicResource, StorageTopicFactory<StorageTopic> topicFactory
    ) {
        TopicKind kind = TopicKind.Main;
        String region = "local";

        String internalTopicName = internalTopicName(varadhiTopicName, kind, region, null);
        StorageTopic storageTopic = topicFactory.getTopic(internalTopicName, topicResource.getCapacityPolicy());
        return new InternalTopic(
                internalTopicName,
                TopicKind.Main,
                "local",
                null,
                ProduceStatus.Active,
                storageTopic
        );
    }

    private static String internalTopicName(
            String varadhiTopicName, TopicKind kind, String region, String sourceRegion
    ) {
        //internal Topic FQDN format is "<varadhiTopicName>.<kind>.<region>[.<sourceRegion>]
        //should be unique w.r.to <varadhiTopicName>
        if (null == sourceRegion) {
            return String.format("%s.%s.%s", varadhiTopicName, kind, region);
        } else {
            return String.format("%s.%s.%s.%s", varadhiTopicName, kind, region, sourceRegion);
        }
    }

    public enum TopicKind {
        Main,
        Replica
    }

    public enum ProduceStatus {
        Active,
        InActive
    }


    public enum StorageKind {
        Pulsar,
        Kafka,
        Meta
    }
}

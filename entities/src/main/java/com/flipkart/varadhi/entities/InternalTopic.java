package com.flipkart.varadhi.entities;


import io.netty.util.internal.StringUtil;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class InternalTopic {

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



    private String region;
    private TopicKind topicKind;
    private ProduceStatus status;
    private String name;
    private StorageTopic storageTopic;

    private String primaryZone; //zone this topic will be replicating from, in case it is replica topic.
    //TODO::check if reference to primary topic also needs to be kept.

    public static List<InternalTopic> from(TopicResource topicResource, StorageTopicFactory<StorageTopic> topicFactory) {
        InternalTopic it1 = new InternalTopic();
        it1.setTopicKind(TopicKind.Main);
        it1.setRegion("local");
        it1.setPrimaryZone(null);
        it1.setStatus(ProduceStatus.Active);
        it1.name = it1.internalTopicName(topicResource.getName());
        StorageTopic st = topicFactory.get(topicResource.getName(), topicResource.getCapacityPolicy());
        it1.setStorageTopic(st);
        return List.of(it1);
    }



    private String internalTopicName(String varadhiTopicName) {
        //internal Topic FQDN format is "<tenant>.<project>.<topicName>.<kind>[.<primaryZone>]
        //This is not the storage topic name, that should be defined by StorageTopic implementation.
        List<String> parts =  Arrays.asList(region, "<tenant>", "<project>", varadhiTopicName, topicKind.name());
        if (!StringUtil.isNullOrEmpty(primaryZone)) {
            parts.add(primaryZone);
        }
        return String.join(".", parts);
    }
}

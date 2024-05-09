package com.flipkart.varadhi.cluster.messages;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;

@Getter
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@messageType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SubscriptionMessage.class, name = "subMessage"),
        @JsonSubTypes.Type(value = ShardMessage.class, name = "shardMessage"),
})
public class ClusterMessage {
    String id;
    long timeStamp;

    public ClusterMessage() {
        this.id = java.util.UUID.randomUUID().toString();
        this.timeStamp = System.currentTimeMillis();
    }

    public ClusterMessage(String id, long timeStamp) {
        this.id = id;
        this.timeStamp = timeStamp;
    }
}

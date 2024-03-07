package com.flipkart.varadhi.core.cluster.messages;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.Getter;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,  property = "@messageType")
@Getter
public class ClusterMessage {
    String id;
    long timeStamp;

    public ClusterMessage() {
        this.id = java.util.UUID.randomUUID().toString();
        this.timeStamp = System.currentTimeMillis();
    }
}

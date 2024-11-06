package com.flipkart.varadhi.qos.entity;


import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class TopicLoadInfo {
    // current millis
    private String clientId; // todo(rl): maybe optional?
    private long from;
    private long to;
    private TrafficData topicLoad; // topic to incoming traffic map

    @Override
    public String toString() {
        return "\nTopicLoadInfo{" +
                "clientId='" + clientId + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", topicLoad=" + topicLoad +
                "}\n";
    }
}

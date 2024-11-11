package com.flipkart.varadhi.qos.entity;

public record TopicLoadInfo(
        String clientId, // todo(rl): maybe optional?
        long from,
        long to,
        TrafficData topicLoad
) {
}

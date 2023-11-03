package com.flipkart.varadhi.entities;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProduceContext {
    private RequestContext requestContext;
    private TopicContext topicContext;

    public ProduceContext(RequestContext requestContext, TopicContext topicContext) {
        this.requestContext = requestContext;
        this.topicContext = topicContext;
    }


    @Getter
    @Setter
    public static class RequestContext {
        String produceIdentity;
        String requestChannel;
        String remoteHost;
        long requestTimestamp;
        long bytesReceived;
        String serviceHost;
    }


    @Getter
    @Setter
    public static class TopicContext {
        // It will be good to add org and team as context as well.
        // But this info is not available by default in the produce path, and would require cache to be maintained.
        // Not including for now, evaluate later.
        String region;
        String topicName;
        String projectName;
    }
}

package com.flipkart.varadhi.entities;

import io.micrometer.core.instrument.Timer;
import io.vertx.core.http.HttpMethod;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class ProduceContext {

    private UserContext userContext;
    private RequestContext requestContext;
    private TopicContext topicContext;

    //TODO::Discuss Is timer better or different timestamp for start/stop better.
    private Timer timer;

    public ProduceContext(UserContext userContext, RequestContext requestContext, TopicContext topicContext) {
        this.userContext = userContext;
        this.requestContext = requestContext;
        this.topicContext = topicContext;
    }


    @Getter
    @Setter
    public static class RequestContext {
        Map<String, String> headers = new HashMap<>();
        HttpMethod httpMethod;
        String requestPath;
        String remoteHost;
        String absoluteUri;
        long requestTimestamp;
        long bytesReceived;
    }


    @Getter
    @Setter
    public static class TopicContext {
        // It will be good to add org an team as context as well.
        // But this info is not available by default in the produce path, and would require cache to be maintained.
        // Not including for now, evaluate later.
        String region;
        String topicName;
        String projectName;
    }
}

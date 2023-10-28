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
    }


    @Getter
    @Setter
    public static class TopicContext {
        String region;
        String org;
        String team;
        String project;
        String topic;

        public void setProjectAttributes(Project project) {
            this.org = project.getOrg();
            this.team = project.getTeam();
            this.project = project.getName();
        }
    }
}

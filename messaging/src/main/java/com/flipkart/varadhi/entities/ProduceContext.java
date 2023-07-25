package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.auth.user.UserContext;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.http.HttpMethod;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class ProduceContext {
    //TODO:: discuss should ProduceContext be in common | entities | messaging.
    private UserContext userContext;
    private RequestContext requestContext;
    private ClusterContext clusterContext;

    //TODO::Discuss Is timer better or different timestamp for start/stop better.
    private Timer timer;

    public ProduceContext() {
    }

    @Getter
    @Setter
    public static class RequestContext {
        Map<String, String> headers;
        HttpMethod httpMethod;
        String requestPath;
        String remoteHost;
        String absoluteUri;
        long requestTimestamp;
        int msgSize;
        long bytesSend;
    }


    @Getter
    @Setter
    public static class ClusterContext {
        String produceZone;
    }
}

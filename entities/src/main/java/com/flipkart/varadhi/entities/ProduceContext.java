package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.MessageConstants;
import com.flipkart.varadhi.auth.user.UserContext;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.REQUEST_PATH_PARAM_TOPIC;

@Getter
@Setter
public class ProduceContext {

    private UserContext userContext;
    private RequestContext requestContext;
    private TopicContext topicContext;

    //TODO::Discuss Is timer better or different timestamp for start/stop better.
    private Timer timer;

    public ProduceContext(RoutingContext ctx, String produceRegion) {
        buildUserContext(ctx.user());
        buildRequestContext(ctx.request());
        buildTopicContext(ctx.request(), produceRegion);
    }

    private void buildUserContext(User user) {
        this.userContext = user == null ? null : new VertxUserContext(user);
    }

    private void buildRequestContext(HttpServerRequest request) {
        this.requestContext = new RequestContext();
        this.requestContext.setRequestPath(request.path());
        this.requestContext.setAbsoluteUri(request.absoluteURI());
        this.requestContext.setRequestTimestamp(System.currentTimeMillis());
        request.headers().forEach((key, value) -> this.requestContext.headers.put(key, value));
        this.requestContext.setBytesReceived(request.bytesRead());
        this.requestContext.setHttpMethod(request.method());
        String remoteHost = request.remoteAddress().host();
        String xfwdedHeaderValue = request.getHeader(MessageConstants.HEADER_X_FWDED_FOR);
        if (null != xfwdedHeaderValue && !xfwdedHeaderValue.isEmpty()) {
            // This could be multivalued (comma separated), take the original initiator i.e. left most in the list.
            String[] proxies = xfwdedHeaderValue.trim().split(",");
            if (!proxies[0].isBlank()) {
                remoteHost = proxies[0];
            }
        }
        this.requestContext.setRemoteHost(remoteHost);
    }

    private void buildTopicContext(HttpServerRequest request, String produceRegion) {
        this.topicContext = new TopicContext();
        this.topicContext.setRegion(produceRegion);
        this.topicContext.setTopicName(request.getParam(REQUEST_PATH_PARAM_TOPIC));
        this.topicContext.setProjectName(request.getParam(REQUEST_PATH_PARAM_PROJECT));
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

package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.MessageConstants;
import com.flipkart.varadhi.auth.user.UserContext;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class ProduceContext {

    private UserContext userContext;
    private RequestContext requestContext;
    private ClusterContext clusterContext;

    //TODO::Discuss Is timer better or different timestamp for start/stop better.
    private Timer timer;

    public ProduceContext(RoutingContext ctx, String deployedRegion) {
        this.userContext = ctx.user() == null ? null : new VertxUserContext(ctx.user());
        this.clusterContext = new ClusterContext();
        this.clusterContext.setProduceRegion(deployedRegion);
        this.requestContext = new RequestContext();
        this.requestContext.setRequestPath(ctx.request().path());
        this.requestContext.setAbsoluteUri(ctx.request().absoluteURI());
        this.requestContext.setRequestTimestamp(System.currentTimeMillis());
        ctx.request().headers().forEach((key, value) -> this.requestContext.headers.put(key, value));
        this.requestContext.setBytesSend(ctx.request().bytesRead());
        this.requestContext.setHttpMethod(ctx.request().method());
        String remoteHost = ctx.request().remoteAddress().host();
        String xfwdedHeaderValue = ctx.request().getHeader(MessageConstants.HEADER_X_FWDED_FOR).trim();
        if (null != xfwdedHeaderValue && !xfwdedHeaderValue.isEmpty()) {
            // This could be multivalued (comma separated), take the original initiator i.e. left most in the list.
            String[] proxies = xfwdedHeaderValue.split(",");
            remoteHost = proxies[0];
        }
        this.requestContext.setRemoteHost(remoteHost);
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
        long bytesSend;
    }

    @Getter
    @Setter
    public static class ClusterContext {
        String produceRegion;
    }
}

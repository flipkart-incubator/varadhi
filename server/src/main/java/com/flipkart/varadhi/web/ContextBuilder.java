package com.flipkart.varadhi.web;

import com.flipkart.varadhi.core.entities.ApiContext;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;


import static com.flipkart.varadhi.Constants.API_CONTEXT_KEY;
import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_PRODUCE_IDENTITY;
import static com.flipkart.varadhi.MessageConstants.API_PROTOCOL_HTTP;
import static com.flipkart.varadhi.core.entities.ApiContext.*;
import static com.flipkart.varadhi.entities.StandardHeaders.*;

public class ContextBuilder implements RouteConfigurator {
    private final ProjectService projectService;
    private final String deployedRegion;
    private final String serviceHost;

    public ContextBuilder(ProjectService projectService, String deployedRegion, String serviceHost) {
        this.projectService = projectService;
        this.deployedRegion = deployedRegion;
        this.serviceHost = serviceHost;
    }

    public void configure(Route route, RouteDefinition routeDef) {
        route.handler((ctx) -> buildApiContext(ctx, routeDef.getName()));
    }
    public void buildApiContext(RoutingContext ctx, String apiName) {
        ApiContext api = new ApiContext();
        api.put(API_NAME, apiName);
        buildRequestContext(ctx, api);
        buildProjectContext(ctx, api);
        buildTopicContext(ctx, api);
        buildMessageContext(ctx, api);
        ctx.put(API_CONTEXT_KEY, api);
        ctx.next();
    }


    private void buildRequestContext(RoutingContext ctx, ApiContext api) {
        api.put(IDENTITY, ctx.user() == null ? ANONYMOUS_PRODUCE_IDENTITY : ctx.user().subject());
        api.put(REGION, deployedRegion);
        api.put(REQUEST_CHANNEL, API_PROTOCOL_HTTP);
        api.put(SERVICE_HOST, serviceHost);
        api.put(REMOTE_HOST, getRemoteHost(ctx));
        api.put(START_TIME, System.currentTimeMillis());
    }

    private void buildProjectContext(RoutingContext ctx, ApiContext api){
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        if (null != projectName) {
            // this will have impact in terms of failure path.
            Project project = projectService.getCachedProject(projectName);
            api.put(ORG, project.getOrg());
            api.put(TEAM, project.getTeam());
            api.put(PROJECT, project.getName());
        }else{
            addFromPathParam(ctx, api, REQUEST_PATH_PARAM_ORG, ORG);
            addFromPathParam(ctx, api, REQUEST_PATH_PARAM_TEAM, TEAM);
        }
    }

    private void buildTopicContext(RoutingContext ctx, ApiContext api) {
        addFromPathParam(ctx, api, REQUEST_PATH_PARAM_TOPIC, TOPIC);
    }

    private void buildMessageContext(RoutingContext ctx, ApiContext api) {
        if (addFromHeader(ctx, api, MESSAGE_ID, MESSAGE)) {
            addFromHeader(ctx, api, GROUP_ID, GROUP);
            api.put(BYTES_RECEIVED, ctx.body().length());
        }
    }

    private boolean addFromPathParam(RoutingContext ctx, ApiContext api, String paramName, String keyName) {
        String value = ctx.pathParam(paramName);
        if (null != value) {
            api.put(keyName, value);
            return true;
        }
        return false;
    }

    private boolean addFromHeader(RoutingContext ctx, ApiContext api, String headerName, String keyName) {
        String value = ctx.request().headers().get(headerName);
        if (null != value) {
            api.put(keyName, value);
            return true;
        }
        return false;
    }

    private String getRemoteHost(RoutingContext ctx) {
        String remoteHost = ctx.request().remoteAddress().host();
        String xForwardedForValue = ctx.request().getHeader(FORWARDED_FOR);
        if (null != xForwardedForValue && !xForwardedForValue.isEmpty()) {
            // This could be multivalued (comma separated), take the original initiator i.e. left most in the list.
            String[] proxies = xForwardedForValue.trim().split(",");
            if (!proxies[0].isBlank()) {
                remoteHost = proxies[0];
            }
        }
        return remoteHost;
    }

}

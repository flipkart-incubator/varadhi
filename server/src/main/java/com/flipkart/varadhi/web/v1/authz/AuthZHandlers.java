package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.IAMPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.AuthZService;
import com.flipkart.varadhi.utils.AuthZHelper;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Stream;

import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.utils.AuthZHelper.AUTH_RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.AuthZHelper.toResponse;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class AuthZHandlers implements RouteProvider {

    private final AuthZService authZService;

    public AuthZHandlers(AuthZService authZService) {
        this.authZService = authZService;
    }

    private List<RouteDefinition> getPolicyHandlers() {
        return new SubRoutes(
                "/v1",
                List.of(
                        RouteDefinition.get("/orgs/:org/policy")
                                .blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_GET, "{org}"))
                                .build(this.getIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.put("/orgs/:org/policy")
                                .hasBody().blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}"))
                                .build(this.setIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.delete("/orgs/:org/policy")
                                .blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}"))
                                .build(this.deleteIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.get("/orgs/:org/teams/:team/policy")
                                .blocking().authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_GET, "{org}/{team}"))
                                .build(this.getIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.put("/orgs/:org/teams/:team/policy")
                                .hasBody().blocking().authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}/{team}"))
                                .build(this.setIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.delete("/orgs/:org/teams/:team/policy")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}/{team}"))
                                .build(this.deleteIAMPolicyHandler(ResourceType.TEAM)),
                        // TODO: permission authz for project and topic
                        RouteDefinition.get("/projects/:project/policy")
                                .blocking().authenticated()
                                .build(this.getIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.put("/projects/:project/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.delete("/projects/:project/policy")
                                .blocking().authenticated()
                                .build(this.deleteIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.get("/projects/:project/topics/:topic/policy")
                                .blocking().authenticated()
                                .build(this.getIAMPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.put("/projects/:project/topics/:topic/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIAMPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.delete("/projects/:project/topics/:topic/policy")
                                .blocking().authenticated()
                                .build(this.deleteIAMPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.get("/projects/:project/subscriptions/:subscription/policy")
                                .blocking().authenticated()
                                .build(this.getIAMPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.put("/projects/:project/subscriptions/:subscription/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIAMPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.delete("/projects/:project/subscriptions/:subscription/policy")
                                .blocking().authenticated()
                                .build(this.deleteIAMPolicyHandler(ResourceType.SUBSCRIPTION))
                )
        ).get();
    }

    @Override
    public List<RouteDefinition> get() {
        return Stream.of(
                getPolicyHandlers()
        ).flatMap(List::stream).toList();
    }

    public Handler<RoutingContext> getIAMPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IAMPolicyResponse response = toResponse(authZService.getIAMPolicy(resourceType, resourceId));
            routingContext.endApiWithResponse(response);
        };
    }

    public Handler<RoutingContext> setIAMPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IAMPolicyRequest policyForSubject = routingContext.body().asValidatedPojo(IAMPolicyRequest.class);
            IAMPolicyResponse updated =
                    toResponse(authZService.setIAMPolicy(resourceType, resourceId, policyForSubject));
            routingContext.endApiWithResponse(updated);
        };
    }

    public Handler<RoutingContext> deleteIAMPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            authZService.deleteIAMPolicy(resourceType, resourceId);
            routingContext.end();
        };
    }

    public void getAllIAMPolicy(RoutingContext routingContext) {
        List<IAMPolicyResponse> response = authZService.getAll().stream().map(AuthZHelper::toResponse).toList();
        routingContext.endApiWithResponse(response);
    }

    private String getResourceIdFromPath(RoutingContext ctx, ResourceType resourceType) {
        return switch (resourceType) {
            case ORG -> ctx.pathParam(REQUEST_PATH_PARAM_ORG);
            case TEAM -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_ORG),
                    ctx.pathParam(REQUEST_PATH_PARAM_TEAM)
            );
            case PROJECT -> ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
            case TOPIC -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_PROJECT),
                    ctx.pathParam(REQUEST_PATH_PARAM_TOPIC)
            );
            case SUBSCRIPTION -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_PROJECT),
                    ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION)
            );
            case IAM_POLICY -> throw new IllegalArgumentException("IAM Policy is not a resource");
        };
    }
}

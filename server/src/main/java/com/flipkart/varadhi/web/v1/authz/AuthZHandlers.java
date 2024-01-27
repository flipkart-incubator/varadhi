package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.services.AuthZService;
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

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class AuthZHandlers implements RouteProvider {

    public static final String AUTH_RESOURCE_NAME_SEPARATOR = ":";

    public static final String REQUEST_PATH_PARAM_RESOURCE = "resource";
    public static final String REQUEST_PATH_PARAM_RESOURCE_TYPE = "resource_type";

    private final AuthZService authZService;

    public AuthZHandlers(AuthZService authZService) {
        this.authZService = authZService;
    }

    private List<RouteDefinition> getDebugHandlers() {
        return new SubRoutes(
                "/v1/authz/debug",
                List.of(
                        RouteDefinition.get("")
                                .authenticated().blocking()
                                .build(this::getAllRoleBindingNodes),
                        RouteDefinition.get("/:resource_type/:resource")
                                .authenticated().blocking()
                                .build(this::findRoleBindingNode),
                        RouteDefinition.delete("/:resource_type/:resource")
                                .authenticated().blocking()
                                .build(this::deleteRoleBindingNode)
                )
        ).get();
    }

    private List<RouteDefinition> getPolicyHandlers() {
        return new SubRoutes(
                "/v1",
                List.of(
                        RouteDefinition.get("/orgs/:org/policy")
                                .authenticated().blocking()
                                .build(this.getIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.put("/orgs/:org/policy")
                                .hasBody().authenticated().blocking()
                                .build(this.setIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.get("/orgs/:org/teams/:team/policy")
                                .authenticated().blocking()
                                .build(this.getIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.put("/orgs/:org/teams/:team/policy")
                                .hasBody().authenticated().blocking()
                                .build(this.setIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.get("/projects/:project/policy")
                                .authenticated().blocking()
                                .build(this.getIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.put("/projects/:project/policy")
                                .hasBody().authenticated().blocking()
                                .build(this.setIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.get("/projects/:project/topics/:topic/policy")
                                .authenticated().blocking()
                                .build(this.getIAMPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.put("/projects/:project/topics/:topic/policy")
                                .hasBody().authenticated().blocking()
                                .build(this.setIAMPolicyHandler(ResourceType.TOPIC))
                )
        ).get();
    }

    @Override
    public List<RouteDefinition> get() {
        return Stream.of(
                getDebugHandlers(),
                getPolicyHandlers()
        ).flatMap(List::stream).toList();
    }

    public Handler<RoutingContext> getIAMPolicyHandler(ResourceType resourceType) {
        return (routingContext) -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            RoleBindingNode policy = getIAMPolicy(resourceType, resourceId);
            routingContext.endApiWithResponse(policy);
        };
    }

    public RoleBindingNode getIAMPolicy(ResourceType resourceType, String resourceId) {
        return authZService.getIAMPolicy(resourceType, resourceId);
    }

    public Handler<RoutingContext> setIAMPolicyHandler(ResourceType resourceType) {
        return (routingContext) -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IAMPolicyRequest policyForSubject = routingContext.body().asValidatedPojo(IAMPolicyRequest.class);
            RoleBindingNode updated = setIAMPolicy(resourceType, resourceId, policyForSubject);
            routingContext.endApiWithResponse(updated);
        };
    }

    public RoleBindingNode setIAMPolicy(
            ResourceType resourceType, String resourceId, IAMPolicyRequest policyForSubject
    ) {
        return authZService.setIAMPolicy(resourceType, resourceId, policyForSubject);
    }

    public void getAllRoleBindingNodes(RoutingContext routingContext) {
        List<RoleBindingNode> roleBindings = authZService.getAllRoleBindingNodes();
        routingContext.endApiWithResponse(roleBindings);
    }

    public void findRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        String resourceType = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE_TYPE);
        RoleBindingNode node = authZService.findRoleBindingNode(ResourceType.valueOf(resourceType), resourceId);
        routingContext.endApiWithResponse(node);
    }

    public void deleteRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        String resourceType = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE_TYPE);
        authZService.deleteRoleBindingNode(ResourceType.valueOf(resourceType), resourceId);
        routingContext.endApi();
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
            case SUBSCRIPTION -> ctx.pathParam("sub");
        };
    }
}

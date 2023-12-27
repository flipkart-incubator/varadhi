package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.entities.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.AuthZService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_RESOURCE;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_RESOURCE_TYPE;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class AuthZHandlers implements RouteProvider {
    private final AuthZService authZService;

    public AuthZHandlers(AuthZService authZService) {
        this.authZService = authZService;
    }

    private List<RouteDefinition> getDebugHandlers() {
        return new SubRoutes(
                "/v1/authz/debug",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getAllRoleBindingNodes,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:resource_type/:resource",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::findRoleBindingNode,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:resource_type/:resource",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::deleteRoleBindingNode,
                                true,
                                Optional.empty()
                        )
                )
        ).get();
    }

    public List<RouteDefinition> getPolicyHandlers() {
        return new SubRoutes(
                "/v1",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/orgs/:org/policy",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this.getIAMPolicyHandler(ResourceType.ORG),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "/orgs/:org/policy",
                                Set.of(hasBody, authenticated),
                                new LinkedHashSet<>(),
                                this.setIAMPolicyHandler(ResourceType.ORG),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/orgs/:org/teams/:team/policy",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this.getIAMPolicyHandler(ResourceType.TEAM),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "/orgs/:org/teams/:team/policy",
                                Set.of(hasBody, authenticated),
                                new LinkedHashSet<>(),
                                this.setIAMPolicyHandler(ResourceType.TEAM),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/projects/:project/policy",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this.getIAMPolicyHandler(ResourceType.PROJECT),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "/projects/:project/policy",
                                Set.of(hasBody, authenticated),
                                new LinkedHashSet<>(),
                                this.setIAMPolicyHandler(ResourceType.PROJECT),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/projects/:project/topics/:topic/policy",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this.getIAMPolicyHandler(ResourceType.TOPIC),
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "/projects/:project/topics/:topic/policy",
                                Set.of(hasBody, authenticated),
                                new LinkedHashSet<>(),
                                this.setIAMPolicyHandler(ResourceType.TOPIC),
                                true,
                                Optional.empty()
                        )
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
            String resourceId = routingContext.getResourceIdFromPath(resourceType);
            RoleBindingNode policy = getIAMPolicy(resourceType, resourceId);
            routingContext.endApiWithResponse(policy);
        };
    }

    public RoleBindingNode getIAMPolicy(ResourceType resourceType, String resourceId) {
        return authZService.getIAMPolicy(resourceType, resourceId);
    }

    public Handler<RoutingContext> setIAMPolicyHandler(ResourceType resourceType) {
        return (routingContext) -> {
            String resourceId = routingContext.getResourceIdFromPath(resourceType);
            IAMPolicyRequest policyForSubject = routingContext.body().asPojo(IAMPolicyRequest.class);
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
}

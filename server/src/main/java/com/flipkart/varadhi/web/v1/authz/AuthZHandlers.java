package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.RoleAssignmentRequest;
import com.flipkart.varadhi.auth.RoleBindingNode;
import com.flipkart.varadhi.services.AuthZService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_RESOURCE;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class AuthZHandlers implements RouteProvider {
    private final AuthZService authZService;

    public AuthZHandlers(AuthZService authZService) {
        this.authZService = authZService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/authz/bindings",
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
                                "/:resource",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getRoleBindingNode,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::setIAMPolicy,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:resource",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::deleteRoleBindingNode,
                                true,
                                Optional.empty()
                        )
                )
        ).get();
    }

    public void getAllRoleBindingNodes(RoutingContext routingContext) {
        List<RoleBindingNode> roleBindings = authZService.getAllRoleBindingNodes();
        routingContext.endApiWithResponse(roleBindings);
    }

    public void getRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        RoleBindingNode roleBindingNode = authZService.getRoleBindingNode(resourceId);
        routingContext.endApiWithResponse(roleBindingNode);
    }

    public void setIAMPolicy(RoutingContext routingContext) {
        RoleAssignmentRequest binding = routingContext.body().asPojo(RoleAssignmentRequest.class);
        RoleBindingNode node = authZService.setIAMPolicy(binding);
        routingContext.endApiWithResponse(node);
    }

    public void deleteRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        authZService.deleteRoleBindingNode(resourceId);
        routingContext.endApi();
    }
}

package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.RoleAssignmentUpdate;
import com.flipkart.varadhi.auth.RoleBindingNode;
import com.flipkart.varadhi.services.DefaultAuthZService;
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
public class DefaultAuthZHandlers implements RouteProvider {
    private final DefaultAuthZService defaultAuthZService;

    public DefaultAuthZHandlers(DefaultAuthZService defaultAuthZService) {
        this.defaultAuthZService = defaultAuthZService;
    }


    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/authz/rbs",
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
                                this::updateRoleAssignment,
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

    private void getAllRoleBindingNodes(RoutingContext routingContext) {
        List<RoleBindingNode> roleBindings = defaultAuthZService.getAllRoleBindingNodes();
        routingContext.endApiWithResponse(roleBindings);
    }

    private void getRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        RoleBindingNode roleBindingNode = defaultAuthZService.getRoleBindingNode(resourceId);
        routingContext.endApiWithResponse(roleBindingNode);
    }

    private void updateRoleAssignment(RoutingContext routingContext) {
        RoleAssignmentUpdate binding = routingContext.body().asPojo(RoleAssignmentUpdate.class);
        RoleBindingNode node = defaultAuthZService.updateRoleAssignment(binding);
        routingContext.endApiWithResponse(node);
    }

    private void deleteRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        defaultAuthZService.deleteRoleBindingNode(resourceId);
        routingContext.endApi();
    }
}

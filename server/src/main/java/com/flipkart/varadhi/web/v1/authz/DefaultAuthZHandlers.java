package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.auth.Role;
import com.flipkart.varadhi.services.DefaultAuthZService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Iterables;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_ROLE;
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
                "/v1/authz",
                StreamSupport.stream(
                                Iterables.concat(
                                                getRolesRouteDefinitions(),
                                                getRoleBindingsRouteDefinitions()
                                        )
                                        .spliterator(), false)
                        .toList()
        ).get();
    }

    private List<RouteDefinition> getRolesRouteDefinitions() {
        return new SubRoutes(
                "/roles",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getAllRoles,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:role",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getRole,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::createRole,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::updateRole,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:role",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::deleteRole,
                                true,
                                Optional.empty()
                        )
                )
        ).get();
    }

    private List<RouteDefinition> getRoleBindingsRouteDefinitions() {
        return new SubRoutes(
                "/rbs",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getAllRoleBindings,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:rb",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getRoleBinding,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::createRoleBinding,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::updateRoleBinding,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:rb",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::deleteRoleBinding,
                                true,
                                Optional.empty()
                        )
                )
        ).get();
    }

    private void createRole(RoutingContext routingContext) {
        Role role = routingContext.body().asPojo(Role.class);
        Role createdRole = defaultAuthZService.createRole(role);
        routingContext.endApiWithResponse(createdRole);
    }

    private void getRole(RoutingContext routingContext) {
        String roleName = routingContext.pathParam(REQUEST_PATH_PARAM_ROLE);
        Role role = defaultAuthZService.getRole(roleName);
        routingContext.endApiWithResponse(role);
    }

    private void getAllRoles(RoutingContext routingContext) {
        List<Role> roles = defaultAuthZService.getRoles();
        routingContext.endApiWithResponse(roles);
    }

    private void deleteRole(RoutingContext routingContext) {
        String roleName = routingContext.pathParam(REQUEST_PATH_PARAM_ROLE);
        defaultAuthZService.deleteRole(roleName);
        routingContext.endApi();
    }

    private void updateRole(RoutingContext routingContext) {
        Role role = routingContext.body().asPojo(Role.class);
        Role updatedRole = defaultAuthZService.updateRole(role);
        routingContext.endApiWithResponse(updatedRole);
    }

    private void createRoleBinding(RoutingContext routingContext) {
    }

    private void getRoleBinding(RoutingContext routingContext) {
    }

    private void getAllRoleBindings(RoutingContext routingContext) {
    }

    private void updateRoleBinding(RoutingContext routingContext) {
    }

    private void deleteRoleBinding(RoutingContext routingContext) {
    }
}

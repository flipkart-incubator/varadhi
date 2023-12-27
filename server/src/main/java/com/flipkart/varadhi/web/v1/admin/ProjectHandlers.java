package com.flipkart.varadhi.web.v1.admin;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.services.ProjectService;
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

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class ProjectHandlers implements RouteProvider {
    private final ProjectService projectService;

    public ProjectHandlers(ProjectService projectService) {
        this.projectService = projectService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:project",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(PROJECT_GET, "{project}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::create,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::update,
                                true,
                                Optional.of(PermissionAuthorization.of(PROJECT_UPDATE, ""))
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:project",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(PROJECT_DELETE, "{project}"))
                        )
                )
        ).get();
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        Project project = projectService.getProject(projectName);
        ctx.endApiWithResponse(project);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Authz check need to be explicit here.
        Project project = ctx.body().asPojo(Project.class);
        Project createdProject = projectService.createProject(project);
        ctx.endApiWithResponse(createdProject);
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        //TODO::No topics and subscriptions for this project.
        projectService.deleteProject(projectName);
        ctx.endApi();
    }

    public void update(RoutingContext ctx) {
        //TODO:: Authz check need to be explicit here.
        Project project = ctx.body().asPojo(Project.class);
        Project updatedProject = projectService.updateProject(project);
        ctx.endApiWithResponse(updatedProject);
    }
}

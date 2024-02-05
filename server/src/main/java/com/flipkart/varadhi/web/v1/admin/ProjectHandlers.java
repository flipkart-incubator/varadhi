package com.flipkart.varadhi.web.v1.admin;


import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

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
                        RouteDefinition.get("GetProject", "/:project")
                                .authorize(PROJECT_GET, "{project}")
                                .build(this::get),
                        RouteDefinition.post("CreateProject", "")
                                .hasBody()
                                .build(this::create),
                        RouteDefinition.put("UpdateProject", "")
                                .hasBody()
                                .authorize(PROJECT_UPDATE, "")
                                .build(this::update),
                        RouteDefinition.delete("DeleteProject", "/:project")
                                .authorize(PROJECT_DELETE, "{project}")
                                .build(this::delete)
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

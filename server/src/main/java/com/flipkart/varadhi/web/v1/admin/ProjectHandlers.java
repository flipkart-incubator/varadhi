package com.flipkart.varadhi.web.v1.admin;


import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class ProjectHandlers implements RouteProvider {
    public static final String API_NAME = "Project";
    private final ProjectService projectService;

    public ProjectHandlers(ProjectService projectService) {
        this.projectService = projectService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects",
            List.of(
                RouteDefinition.get("get", API_NAME, "/:project")
                               .authorize(PROJECT_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post("create", API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setProject)
                               .authorize(PROJECT_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.put("update", API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setProject)
                               .authorize(PROJECT_UPDATE)
                               .build(this::getHierarchies, this::update),
                RouteDefinition.delete("delete", API_NAME, "/:project")
                               .authorize(PROJECT_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setProject(RoutingContext ctx) {
        Project project = ctx.body().asValidatedPojo(Project.class);
        ctx.put(CONTEXT_KEY_BODY, project);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project project = hasBody ?
            ctx.get(CONTEXT_KEY_BODY) :
            projectService.getCachedProject(ctx.request().getParam(PATH_PARAM_PROJECT));
        return Map.of(ResourceType.PROJECT, new Hierarchies.ProjectHierarchy(project));
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        Project project = projectService.getProject(projectName);
        ctx.endApiWithResponse(project);
    }

    public void create(RoutingContext ctx) {
        Project project = ctx.get(CONTEXT_KEY_BODY);
        Project createdProject = projectService.createProject(project);
        ctx.endApiWithResponse(createdProject);
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        projectService.deleteProject(projectName);
        ctx.endApi();
    }

    public void update(RoutingContext ctx) {
        Project project = ctx.get(CONTEXT_KEY_BODY);
        Project updatedProject = projectService.updateProject(project);
        ctx.endApiWithResponse(updatedProject);
    }
}

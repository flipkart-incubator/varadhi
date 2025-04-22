package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.common.EntityReadCache;
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

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.entities.auth.ResourceAction.PROJECT_CREATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.PROJECT_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.PROJECT_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.PROJECT_UPDATE;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class ProjectHandlers implements RouteProvider {
    private static final String API_NAME = "PROJECT";
    private final ProjectService projectService;
    private final EntityReadCache<Project> projectCache;

    public ProjectHandlers(ProjectService projectService, EntityReadCache<Project> projectCache) {
        this.projectService = projectService;
        this.projectCache = projectCache;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects",
            List.of(
                RouteDefinition.get(GET, API_NAME, "/:project")
                               .authorize(PROJECT_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setProject)
                               .authorize(PROJECT_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.put(UPDATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setProject)
                               .authorize(PROJECT_UPDATE)
                               .build(this::getHierarchies, this::update),
                RouteDefinition.delete(DELETE, API_NAME, "/:project")
                               .authorize(PROJECT_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setProject(RoutingContext ctx) {
        Project project = ctx.body().asValidatedPojo(Project.class);
        ctx.put(REQUEST_BODY, project);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project project = hasBody ?
            ctx.get(REQUEST_BODY) :
            projectService.getCachedProject(ctx.request().getParam(PATH_PARAM_PROJECT));

        return Map.of(ResourceType.PROJECT, new Hierarchies.ProjectHierarchy(project));
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        Project project = projectService.getProject(projectName);
        ctx.endApiWithResponse(project);
    }

    public void create(RoutingContext ctx) {
        Project project = ctx.get(REQUEST_BODY);
        Project createdProject = projectService.createProject(project);
        ctx.endApiWithResponse(createdProject);
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        projectService.deleteProject(projectName);
        ctx.endApi();
    }

    public void update(RoutingContext ctx) {
        Project project = ctx.get(REQUEST_BODY);
        Project updatedProject = projectService.updateProject(project);
        ctx.endApiWithResponse(updatedProject);
    }
}

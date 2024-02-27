package com.flipkart.varadhi.web.v1.authz;


import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.services.ProjectService;
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
public class IamPolicyHandlers implements RouteProvider {

    public static final String AUTH_RESOURCE_NAME_SEPARATOR = ":";

    public static final String REQUEST_PATH_PARAM_RESOURCE = "resource";
    public static final String REQUEST_PATH_PARAM_RESOURCE_TYPE = "resource_type";
    private final ProjectService projectService;
    private final IamPolicyService iamPolicyService;

    public IamPolicyHandlers(ProjectService projectService, IamPolicyService iamPolicyService) {
        this.iamPolicyService = iamPolicyService;
        this.projectService = projectService;
    }

    private List<RouteDefinition> getDebugHandlers() {
        return new SubRoutes(
                "/v1/authz/debug",
                List.of(
                        RouteDefinition.get("GetAllRoleBindingNodes", "")
                                .build(this::getIamHierarchy, this::getAllRoleBindingNodes),
                        RouteDefinition.get("GetRoleBindingNode", "/:resource_type/:resource")
                                .build(this::getIamHierarchy, this::findRoleBindingNode),
                        RouteDefinition.delete("DeleteRoleBindingNode", "/:resource_type/:resource")
                                .build(this::getIamHierarchy, this::deleteRoleBindingNode)
                )
        ).get();
    }

    public ResourceHierarchy getIamHierarchy(RoutingContext ctx, boolean hasBody) {
        String resourceType = ctx.request().getParam("resource_type");
        String resourceName = ctx.request().getParam("resource");
        if (null != resourceType && null != resourceName) {
            return new Hierarchies.IamPolicyHierarchy(resourceType, resourceName);
        }
        return new Hierarchies.RootHierarchy();
    }

    private List<RouteDefinition> getPolicyHandlers() {
        return new SubRoutes(
                "/v1",
                List.of(
                        RouteDefinition.get("GetIAMPolicy", "/orgs/:org/policy")
                                .build(this::getHierarchy, this.getIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.put("SetIAMPolicy", "/orgs/:org/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIAMPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.get("GetIAMPolicy", "/orgs/:org/teams/:team/policy")
                                .build(this::getHierarchy, this.getIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.put("SetIAMPolicy", "/orgs/:org/teams/:team/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIAMPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.get("GetIAMPolicy", "/projects/:project/policy")
                                .build(this::getHierarchy, this.getIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.put("SetIAMPolicy", "/projects/:project/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIAMPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.get("GetIAMPolicy", "/projects/:project/topics/:topic/policy")
                                .build(this::getHierarchy, this.getIAMPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.put("SetIAMPolicy", "/projects/:project/topics/:topic/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIAMPolicyHandler(ResourceType.TOPIC))
                )
        ).get();
    }

    public ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
        String orgName = ctx.request().getParam(PATH_PARAM_ORG);
        String teamName = ctx.request().getParam(PATH_PARAM_TEAM);
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        if (topicName != null) {
            Project project = projectService.getProject(projectName);
            return new Hierarchies.TopicHierarchy(project.getOrg(), project.getTeam(), project.getName(), topicName);
        }
        if (projectName != null) {
            Project project = projectService.getProject(projectName);
            return new Hierarchies.ProjectHierarchy(project.getOrg(), project.getTeam(), project.getName());
        }
        if (teamName != null) {
            return new Hierarchies.TeamHierarchy(orgName, teamName);
        }
        if (orgName != null) {
            return new Hierarchies.OrgHierarchy(orgName);
        }
        return new Hierarchies.RootHierarchy();
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
        return iamPolicyService.getIAMPolicy(resourceType, resourceId);
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
        return iamPolicyService.setIAMPolicy(resourceType, resourceId, policyForSubject);
    }

    public void getAllRoleBindingNodes(RoutingContext routingContext) {
        List<RoleBindingNode> roleBindings = iamPolicyService.getAllRoleBindingNodes();
        routingContext.endApiWithResponse(roleBindings);
    }

    public void findRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        String resourceType = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE_TYPE);
        RoleBindingNode node = iamPolicyService.findRoleBindingNode(ResourceType.valueOf(resourceType), resourceId);
        routingContext.endApiWithResponse(node);
    }

    public void deleteRoleBindingNode(RoutingContext routingContext) {
        String resourceId = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE);
        String resourceType = routingContext.pathParam(REQUEST_PATH_PARAM_RESOURCE_TYPE);
        iamPolicyService.deleteRoleBindingNode(ResourceType.valueOf(resourceType), resourceId);
        routingContext.endApi();
    }

    private String getResourceIdFromPath(RoutingContext ctx, ResourceType resourceType) {
        return switch (resourceType) {
            case ORG -> ctx.pathParam(PATH_PARAM_ORG);
            case TEAM -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(PATH_PARAM_ORG),
                    ctx.pathParam(PATH_PARAM_TEAM)
            );
            case PROJECT -> ctx.pathParam(PATH_PARAM_PROJECT);
            case TOPIC -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(PATH_PARAM_PROJECT),
                    ctx.pathParam(PATH_PARAM_TOPIC)
            );
            case SUBSCRIPTION -> ctx.pathParam("sub");
        };
    }
}

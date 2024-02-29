package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.utils.IamPolicyHelper;
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
import static com.flipkart.varadhi.entities.auth.ResourceAction.IAM_POLICY_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.IAM_POLICY_SET;
import static com.flipkart.varadhi.utils.IamPolicyHelper.AUTH_RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.toResponse;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class IamPolicyHandlers implements RouteProvider {

    private final ProjectService projectService;
    private final IamPolicyService iamPolicyService;

    public IamPolicyHandlers(ProjectService projectService, IamPolicyService iamPolicyService) {
        this.iamPolicyService = iamPolicyService;
        this.projectService = projectService;
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
                                .authorize(IAM_POLICY_GET)
                                .build(this::getHierarchy, this.getIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.put("SetIAMPolicy", "/orgs/:org/policy")
                                .hasBody()
                                .authorize(IAM_POLICY_SET)
                                .build(this::getHierarchy, this.setIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.delete("DeleteIAMPolicy", "/orgs/:org/policy")
                                .authorize(IAM_POLICY_SET)
                                .build(this::getHierarchy, this.deleteIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.get("GetIAMPolicy", "/orgs/:org/teams/:team/policy")
                                .authorize(IAM_POLICY_GET)
                                .build(this::getHierarchy, this.getIamPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.put("SetIAMPolicy", "/orgs/:org/teams/:team/policy")
                                .hasBody()
                                .authorize(IAM_POLICY_SET)
                                .build(this::getHierarchy, this.setIamPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.delete("DeleteIAMPolicy", "/orgs/:org/teams/:team/policy")
                                .authorize(IAM_POLICY_SET)
                                .build(this::getHierarchy, this.deleteIamPolicyHandler(ResourceType.TEAM)),

                        RouteDefinition.get("GetIAMPolicy", "/projects/:project/policy")
                                .build(this::getHierarchy, this.getIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.put("SetIAMPolicy", "/projects/:project/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.delete("DeleteIAMPolicy", "/projects/:project/policy")
                                .build(this::getHierarchy, this.deleteIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.get("GetIAMPolicy", "/projects/:project/topics/:topic/policy")
                                .build(this::getHierarchy, this.getIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.put("SetIAMPolicy", "/projects/:project/topics/:topic/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.delete("DeleteIAMPolicy", "/projects/:project/topics/:topic/policy")
                                .build(this::getHierarchy, this.deleteIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.get("GetIAMPolicy", "/projects/:project/subscriptions/:subscription/policy")
                                .build(this::getHierarchy, this.getIamPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.put("SetIAMPolicy", "/projects/:project/subscriptions/:subscription/policy")
                                .hasBody()
                                .build(this::getHierarchy, this.setIamPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.delete(
                                        "DeleteIAMPolicy", "/projects/:project/subscriptions/:subscription/policy")
                                .build(this::getHierarchy, this.deleteIamPolicyHandler(ResourceType.SUBSCRIPTION))
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
                getPolicyHandlers()
        ).flatMap(List::stream).toList();
    }

    public Handler<RoutingContext> getIamPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IamPolicyResponse response = toResponse(iamPolicyService.getIamPolicy(resourceType, resourceId));
            routingContext.endApiWithResponse(response);
        };
    }

    public Handler<RoutingContext> setIamPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IamPolicyRequest policyForSubject = routingContext.body().asValidatedPojo(IamPolicyRequest.class);
            IamPolicyResponse updated =
                    toResponse(iamPolicyService.setIamPolicy(resourceType, resourceId, policyForSubject));
            routingContext.endApiWithResponse(updated);
        };
    }

    public Handler<RoutingContext> deleteIamPolicyHandler(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            iamPolicyService.deleteIamPolicy(resourceType, resourceId);
            routingContext.end();
        };
    }

    public void getAllIamPolicy(RoutingContext routingContext) {
        List<IamPolicyResponse> response = iamPolicyService.getAll().stream().map(IamPolicyHelper::toResponse).toList();
        routingContext.endApiWithResponse(response);
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
            case SUBSCRIPTION -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(PATH_PARAM_PROJECT),
                    ctx.pathParam(PATH_PARAM_SUBSCRIPTION)
            );
            case IAM_POLICY -> throw new IllegalArgumentException("Iam Policy is not a resource");

        };
    }
}

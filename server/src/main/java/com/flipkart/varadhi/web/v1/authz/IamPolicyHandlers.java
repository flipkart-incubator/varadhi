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
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static com.flipkart.varadhi.utils.IamPolicyHelper.AUTH_RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.toResponse;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class IamPolicyHandlers implements RouteProvider {

    private static final String ORG_POLICY_PATH = "orgs/:org/policy";
    private static final String TEAM_POLICY_PATH = "orgs/:org/teams/:team/policy";
    private static final String PROJECT_POLICY_PATH = "projects/:project/policy";
    private static final String TOPIC_POLICY_PATH = "projects/:project/topics/:topic/policy";
    private static final String SUBSCRIPTION_POLICY_PATH = "projects/:project/subscriptions/:subscription/policy";
    private final ProjectService projectService;
    private final IamPolicyService iamPolicyService;

    public IamPolicyHandlers(ProjectService projectService, IamPolicyService iamPolicyService) {
        this.iamPolicyService = iamPolicyService;
        this.projectService = projectService;
    }

    private static String getResourceIdFromPath(RoutingContext ctx, ResourceType resourceType) {
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

    private static ResourceAction getActionBasedOnResource(ResourceType type) {
        return switch (type) {
            case TOPIC -> TOPIC_IAM_POLICY_GET;
            case SUBSCRIPTION -> SUBSCRIPTION_IAM_POLICY_GET;
            default -> IAM_POLICY_GET;
        };
    }

    private static ResourceAction setActionBasedOnResource(ResourceType type) {
        return switch (type) {
            case TOPIC -> TOPIC_IAM_POLICY_SET;
            case SUBSCRIPTION -> SUBSCRIPTION_IAM_POLICY_SET;
            default -> IAM_POLICY_SET;
        };
    }

    private static ResourceAction deleteActionBasedOnResource(ResourceType type) {
        return switch (type) {
            case TOPIC -> TOPIC_IAM_POLICY_DELETE;
            case SUBSCRIPTION -> SUBSCRIPTION_IAM_POLICY_DELETE;
            default -> IAM_POLICY_DELETE;
        };
    }

    private List<RouteDefinition> getHandlersFor(String path, ResourceType resourceType) {
        return List.of(
                RouteDefinition.get("GetIAMPolicy", path).authorize(getActionBasedOnResource(resourceType))
                        .build(this::getHierarchy, this.getIamPolicyHandler(resourceType)),
                RouteDefinition.put("SetIAMPolicy", path).hasBody().authorize(setActionBasedOnResource(resourceType))
                        .build(this::getHierarchy, this.setIamPolicyHandler(resourceType)),
                RouteDefinition.delete("DeleteIAMPolicy", path).authorize(deleteActionBasedOnResource(resourceType))
                        .build(this::getHierarchy, this.deleteIamPolicyHandler(resourceType))
        );
    }

    private ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
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
            routingContext.endApi();
        };
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/",
                Stream.of(
                        getHandlersFor(ORG_POLICY_PATH, ResourceType.ORG),
                        getHandlersFor(TEAM_POLICY_PATH, ResourceType.TEAM),
                        getHandlersFor(PROJECT_POLICY_PATH, ResourceType.PROJECT),
                        getHandlersFor(TOPIC_POLICY_PATH, ResourceType.TOPIC),
                        getHandlersFor(SUBSCRIPTION_POLICY_PATH, ResourceType.SUBSCRIPTION)
                ).flatMap(List::stream).toList()
        ).get();
    }
}

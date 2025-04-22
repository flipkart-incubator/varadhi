package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
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
import java.util.Map;
import java.util.stream.Stream;

import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.*;
import static com.flipkart.varadhi.entities.Hierarchies.*;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static com.flipkart.varadhi.utils.IamPolicyHelper.AUTH_RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.toResponse;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class IamPolicyHandlers implements RouteProvider {

    private static final String API_NAME = "IAM";

    private static final String ORG_POLICY_PATH = "orgs/:org/policy";
    private static final String TEAM_POLICY_PATH = "orgs/:org/teams/:team/policy";
    private static final String PROJECT_POLICY_PATH = "projects/:project/policy";
    private static final String TOPIC_POLICY_PATH = "projects/:project/topics/:topic/policy";
    private static final String SUBSCRIPTION_POLICY_PATH = "projects/:project/subscriptions/:subscription/policy";
    private final ProjectService projectService;
    private final IamPolicyService iamPolicyService;

    public IamPolicyHandlers(ProjectService projectService, IamPolicyService iamPolicyService) {
        this.projectService = projectService;
        this.iamPolicyService = iamPolicyService;
    }

    private static String getResourceIdFromPath(RoutingContext ctx, ResourceType resourceType) {
        return switch (resourceType) {
            case ROOT -> throw new IllegalArgumentException(
                "ROOT is implicit resource type. No Iam policies supported on it."
            );
            case ORG -> ctx.pathParam(PATH_PARAM_ORG);
            case TEAM -> String.join(
                AUTH_RESOURCE_NAME_SEPARATOR,
                ctx.pathParam(PATH_PARAM_ORG),
                ctx.pathParam(PATH_PARAM_TEAM)
            );
            case PROJECT -> ctx.pathParam(PATH_PARAM_PROJECT);
            case TOPIC -> String.join(
                AUTH_RESOURCE_NAME_SEPARATOR,
                ctx.pathParam(PATH_PARAM_PROJECT),
                ctx.pathParam(PATH_PARAM_TOPIC)
            );
            case SUBSCRIPTION -> String.join(
                AUTH_RESOURCE_NAME_SEPARATOR,
                ctx.pathParam(PATH_PARAM_PROJECT),
                ctx.pathParam(PATH_PARAM_SUBSCRIPTION)
            );
            case IAM_POLICY -> throw new IllegalArgumentException("IamPolicy is not a policy owning resource.");
        };
    }

    private List<RouteDefinition> getHandlersFor(String path, ResourceType resourceType) {
        return List.of(
            RouteDefinition.get(GET, API_NAME, path)
                           .authorize(IAM_POLICY_GET)
                           .build(this::getHierarchies, this.get(resourceType)),
            RouteDefinition.put(SET, API_NAME, path)
                           .hasBody()
                           .authorize(IAM_POLICY_SET)
                           .build(this::getHierarchies, this.set(resourceType)),
            RouteDefinition.delete(DELETE, API_NAME, path)
                           .authorize(IAM_POLICY_DELETE)
                           .build(this::getHierarchies, this.delete(resourceType))
        );
    }

    private Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String orgName = ctx.request().getParam(PATH_PARAM_ORG);
        String teamName = ctx.request().getParam(PATH_PARAM_TEAM);
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (subscriptionName != null) {
            Project project = projectService.getProject(projectName);
            return Map.of(
                ResourceType.IAM_POLICY,
                new IamPolicyHierarchy(new SubscriptionHierarchy(project, subscriptionName))
            );
        }
        if (topicName != null) {
            Project project = projectService.getProject(projectName);
            return Map.of(ResourceType.IAM_POLICY, new IamPolicyHierarchy(new TopicHierarchy(project, topicName)));
        }
        if (projectName != null) {
            return Map.of(
                ResourceType.IAM_POLICY,
                new IamPolicyHierarchy(new ProjectHierarchy(projectService.getProject(projectName)))
            );
        }
        if (teamName != null) {
            return Map.of(ResourceType.IAM_POLICY, new IamPolicyHierarchy(new TeamHierarchy(orgName, teamName)));
        }
        if (orgName != null) {
            return Map.of(ResourceType.IAM_POLICY, new IamPolicyHierarchy(new OrgHierarchy(orgName)));
        }
        return Map.of(ResourceType.IAM_POLICY, new IamPolicyHierarchy(new RootHierarchy()));
    }

    public Handler<RoutingContext> get(ResourceType resourceType) {
        return routingContext -> {
            String policyId = getResourceIdFromPath(routingContext, resourceType);
            IamPolicyResponse response = toResponse(iamPolicyService.getIamPolicy(resourceType, policyId));
            routingContext.endApiWithResponse(response);
        };
    }

    public Handler<RoutingContext> set(ResourceType resourceType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, resourceType);
            IamPolicyRequest policyForSubject = routingContext.body().asValidatedPojo(IamPolicyRequest.class);
            IamPolicyResponse updated = toResponse(
                iamPolicyService.setIamPolicy(resourceType, resourceId, policyForSubject)
            );
            routingContext.endApiWithResponse(updated);
        };
    }

    public Handler<RoutingContext> delete(ResourceType resourceType) {
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

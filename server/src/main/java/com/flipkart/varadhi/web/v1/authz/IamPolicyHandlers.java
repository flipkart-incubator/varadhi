package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.EntityType;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
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

    private static String getResourceIdFromPath(RoutingContext ctx, EntityType entityType) {
        return switch (entityType) {
            case ROOT -> throw new IllegalArgumentException(
                "ROOT is implicit resource type. No Iam policies supported on it."
            );
            case ORG, ORG_FILTER -> ctx.pathParam(PATH_PARAM_ORG);
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

    private List<RouteDefinition> getHandlersFor(String path, EntityType entityType) {
        return List.of(
            RouteDefinition.get(GET, API_NAME, path)
                           .authorize(IAM_POLICY_GET)
                           .build(this::getHierarchies, this.get(entityType)),
            RouteDefinition.put(SET, API_NAME, path)
                           .hasBody()
                           .authorize(IAM_POLICY_SET)
                           .build(this::getHierarchies, this.set(entityType)),
            RouteDefinition.delete(DELETE, API_NAME, path)
                           .authorize(IAM_POLICY_DELETE)
                           .build(this::getHierarchies, this.delete(entityType))
        );
    }

    private Map<EntityType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String orgName = ctx.request().getParam(PATH_PARAM_ORG);
        String teamName = ctx.request().getParam(PATH_PARAM_TEAM);
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (subscriptionName != null) {
            Project project = projectService.getProject(projectName);
            return Map.of(
                EntityType.IAM_POLICY,
                new IamPolicyHierarchy(new SubscriptionHierarchy(project, subscriptionName))
            );
        }
        if (topicName != null) {
            Project project = projectService.getProject(projectName);
            return Map.of(EntityType.IAM_POLICY, new IamPolicyHierarchy(new TopicHierarchy(project, topicName)));
        }
        if (projectName != null) {
            return Map.of(
                EntityType.IAM_POLICY,
                new IamPolicyHierarchy(new ProjectHierarchy(projectService.getProject(projectName)))
            );
        }
        if (teamName != null) {
            return Map.of(EntityType.IAM_POLICY, new IamPolicyHierarchy(new TeamHierarchy(orgName, teamName)));
        }
        if (orgName != null) {
            return Map.of(EntityType.IAM_POLICY, new IamPolicyHierarchy(new OrgHierarchy(orgName)));
        }
        return Map.of(EntityType.IAM_POLICY, new IamPolicyHierarchy(new RootHierarchy()));
    }

    public Handler<RoutingContext> get(EntityType entityType) {
        return routingContext -> {
            String policyId = getResourceIdFromPath(routingContext, entityType);
            IamPolicyResponse response = toResponse(iamPolicyService.getIamPolicy(entityType, policyId));
            routingContext.endApiWithResponse(response);
        };
    }

    public Handler<RoutingContext> set(EntityType entityType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, entityType);
            IamPolicyRequest policyForSubject = routingContext.body().asValidatedPojo(IamPolicyRequest.class);
            IamPolicyResponse updated = toResponse(
                iamPolicyService.setIamPolicy(entityType, resourceId, policyForSubject)
            );
            routingContext.endApiWithResponse(updated);
        };
    }

    public Handler<RoutingContext> delete(EntityType entityType) {
        return routingContext -> {
            String resourceId = getResourceIdFromPath(routingContext, entityType);
            iamPolicyService.deleteIamPolicy(entityType, resourceId);
            routingContext.endApi();
        };
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/",
            Stream.of(
                getHandlersFor(ORG_POLICY_PATH, EntityType.ORG),
                getHandlersFor(TEAM_POLICY_PATH, EntityType.TEAM),
                getHandlersFor(PROJECT_POLICY_PATH, EntityType.PROJECT),
                getHandlersFor(TOPIC_POLICY_PATH, EntityType.TOPIC),
                getHandlersFor(SUBSCRIPTION_POLICY_PATH, EntityType.SUBSCRIPTION)
            ).flatMap(List::stream).toList()
        ).get();
    }
}

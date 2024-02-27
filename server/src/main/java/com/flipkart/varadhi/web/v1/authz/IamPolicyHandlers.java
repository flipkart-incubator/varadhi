package com.flipkart.varadhi.web.v1.authz;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.IamPolicyService;
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
import static com.flipkart.varadhi.utils.IamPolicyHelper.AUTH_RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.toResponse;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class IamPolicyHandlers implements RouteProvider {

    private final IamPolicyService iamPolicyService;

    public IamPolicyHandlers(IamPolicyService iamPolicyService) {
        this.iamPolicyService = iamPolicyService;
    }

    private List<RouteDefinition> getPolicyHandlers() {
        return new SubRoutes(
                "/v1",
                List.of(
                        RouteDefinition.get("/orgs/:org/policy")
                                .blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_GET, "{org}"))
                                .build(this.getIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.put("/orgs/:org/policy")
                                .hasBody().blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}"))
                                .build(this.setIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.delete("/orgs/:org/policy")
                                .blocking()
                                .authenticatedWith(PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}"))
                                .build(this.deleteIamPolicyHandler(ResourceType.ORG)),
                        RouteDefinition.get("/orgs/:org/teams/:team/policy")
                                .blocking().authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_GET, "{org}/{team}"))
                                .build(this.getIamPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.put("/orgs/:org/teams/:team/policy")
                                .hasBody().blocking().authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}/{team}"))
                                .build(this.setIamPolicyHandler(ResourceType.TEAM)),
                        RouteDefinition.delete("/orgs/:org/teams/:team/policy")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(ResourceAction.IAM_POLICY_SET, "{org}/{team}"))
                                .build(this.deleteIamPolicyHandler(ResourceType.TEAM)),
                        // TODO: permission authz for project and topic
                        RouteDefinition.get("/projects/:project/policy")
                                .blocking().authenticated()
                                .build(this.getIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.put("/projects/:project/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.delete("/projects/:project/policy")
                                .blocking().authenticated()
                                .build(this.deleteIamPolicyHandler(ResourceType.PROJECT)),
                        RouteDefinition.get("/projects/:project/topics/:topic/policy")
                                .blocking().authenticated()
                                .build(this.getIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.put("/projects/:project/topics/:topic/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.delete("/projects/:project/topics/:topic/policy")
                                .blocking().authenticated()
                                .build(this.deleteIamPolicyHandler(ResourceType.TOPIC)),
                        RouteDefinition.get("/projects/:project/subscriptions/:subscription/policy")
                                .blocking().authenticated()
                                .build(this.getIamPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.put("/projects/:project/subscriptions/:subscription/policy")
                                .hasBody().blocking().authenticated()
                                .build(this.setIamPolicyHandler(ResourceType.SUBSCRIPTION)),
                        RouteDefinition.delete("/projects/:project/subscriptions/:subscription/policy")
                                .blocking().authenticated()
                                .build(this.deleteIamPolicyHandler(ResourceType.SUBSCRIPTION))
                )
        ).get();
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
            case ORG -> ctx.pathParam(REQUEST_PATH_PARAM_ORG);
            case TEAM -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_ORG),
                    ctx.pathParam(REQUEST_PATH_PARAM_TEAM)
            );
            case PROJECT -> ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
            case TOPIC -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_PROJECT),
                    ctx.pathParam(REQUEST_PATH_PARAM_TOPIC)
            );
            case SUBSCRIPTION -> String.join(AUTH_RESOURCE_NAME_SEPARATOR, ctx.pathParam(REQUEST_PATH_PARAM_PROJECT),
                    ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION)
            );
            case IAM_POLICY -> throw new IllegalArgumentException("Iam Policy is not a resource");
        };
    }
}

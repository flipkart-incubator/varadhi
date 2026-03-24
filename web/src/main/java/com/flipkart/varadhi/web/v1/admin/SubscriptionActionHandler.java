package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.VaradhiSubscriptionService;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.hierarchy.Hierarchies;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.MethodNames.START;
import static com.flipkart.varadhi.common.Constants.MethodNames.STOP;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_UPDATE;

/**
 * Handles actions that are common to both subscription (topic) and queue: start, stop, offset-reset, etc.
 * These actions require only the subscription name and are shared by topic and queue flows.
 * <p>
 * Route definitions for these actions are in this class. This class is a {@link RouteProvider} so
 * action routes are registered separately from {@link SubscriptionHandlers} (CRUD and restore only).
 */
public class SubscriptionActionHandler implements RouteProvider {

    private static final String API_NAME = "SUBSCRIPTION";
    private static final String SUBSCRIPTIONS_PATH = "/v1/projects/:project/subscriptions";

    private final VaradhiSubscriptionService varadhiSubscriptionService;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    public SubscriptionActionHandler(
        VaradhiSubscriptionService varadhiSubscriptionService,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache
    ) {
        this.varadhiSubscriptionService = varadhiSubscriptionService;
        this.projectCache = projectCache;
    }

    /**
     * Resource hierarchies for start/stop routes (no body; subscription from path).
     */
    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project subscriptionProject = projectCache.getOrThrow(ctx.request().getParam(PATH_PARAM_PROJECT)).getEntity();
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (subscriptionName == null) {
            return Map.of(ResourceType.PROJECT, new Hierarchies.ProjectHierarchy(subscriptionProject));
        }

        VaradhiSubscription subscription = varadhiSubscriptionService.getSubscription(getSubscriptionFqn(ctx));
        VaradhiTopicName topicFqn = VaradhiTopicName.parse(subscription.getTopic());
        Project topicProject = projectCache.getOrThrow(topicFqn.getProjectName()).getEntity();
        String topicName = topicFqn.getTopicName();

        return Map.ofEntries(
            Map.entry(
                ResourceType.SUBSCRIPTION,
                new Hierarchies.SubscriptionHierarchy(subscriptionProject, subscriptionName)
            ),
            Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, topicName))
        );
    }

    /**
     * Returns route definitions for actions common to subscription and queue (start, stop, etc.).
     */
    public List<RouteDefinition> getActionRouteDefinitions() {
        return List.of(
            RouteDefinition.post(START, API_NAME, "/:subscription/start")
                           .nonBlocking()
                           .authorize(SUBSCRIPTION_UPDATE)
                           .build(this::getHierarchies, this::start),
            RouteDefinition.post(STOP, API_NAME, "/:subscription/stop")
                           .nonBlocking()
                           .authorize(SUBSCRIPTION_UPDATE)
                           .build(this::getHierarchies, this::stop)
        );
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(SUBSCRIPTIONS_PATH, getActionRouteDefinitions()).get();
    }

    /**
     * Starts a subscription.
     */
    public void start(RoutingContext ctx) {
        executeLifecycleAction(ctx, LifecycleAction.START);
    }

    /**
     * Stops a subscription.
     */
    public void stop(RoutingContext ctx) {
        executeLifecycleAction(ctx, LifecycleAction.STOP);
    }

    private void executeLifecycleAction(RoutingContext ctx, LifecycleAction action) {
        String fqn = getSubscriptionFqn(ctx);
        String identity = Extensions.RoutingContextExtension.getIdentityOrDefault(ctx);
        var future = switch (action) {
            case START -> varadhiSubscriptionService.start(fqn, identity);
            case STOP -> varadhiSubscriptionService.stop(fqn, identity);
        };
        Extensions.RoutingContextExtension.handleResponse(ctx, future);
    }

    private enum LifecycleAction {
        START, STOP
    }

    private static String getSubscriptionFqn(RoutingContext ctx) {
        return SubscriptionResource.buildInternalName(
            ctx.pathParam(PATH_PARAM_PROJECT),
            ctx.pathParam(PATH_PARAM_SUBSCRIPTION)
        );
    }
}

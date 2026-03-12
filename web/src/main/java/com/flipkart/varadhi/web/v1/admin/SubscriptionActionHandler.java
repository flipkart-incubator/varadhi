package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.VaradhiSubscriptionService;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.hierarchy.Hierarchies;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;

import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;

/**
 * Handles subscription lifecycle actions: start and stop.
 */
public class SubscriptionActionHandler {

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
        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        Project topicProject = projectCache.getOrThrow(topicNameSegments[0]).getEntity();
        String topicName = topicNameSegments[1];

        return Map.ofEntries(
            Map.entry(ResourceType.SUBSCRIPTION, new Hierarchies.SubscriptionHierarchy(subscriptionProject, subscriptionName)),
            Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, topicName))
        );
    }

    /**
     * Starts a subscription.
     */
    public void start(RoutingContext ctx) {
        Extensions.RoutingContextExtension.handleResponse(
            ctx,
            varadhiSubscriptionService.start(
                getSubscriptionFqn(ctx),
                Extensions.RoutingContextExtension.getIdentityOrDefault(ctx)
            )
        );
    }

    /**
     * Stops a subscription.
     */
    public void stop(RoutingContext ctx) {
        Extensions.RoutingContextExtension.handleResponse(
            ctx,
            varadhiSubscriptionService.stop(
                getSubscriptionFqn(ctx),
                Extensions.RoutingContextExtension.getIdentityOrDefault(ctx)
            )
        );
    }

    private static String getSubscriptionFqn(RoutingContext ctx) {
        return SubscriptionResource.buildInternalName(
            ctx.pathParam(PATH_PARAM_PROJECT),
            ctx.pathParam(PATH_PARAM_SUBSCRIPTION)
        );
    }
}

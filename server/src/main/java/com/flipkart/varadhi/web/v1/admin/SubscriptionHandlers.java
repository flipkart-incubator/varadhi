package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.entities.Hierarchies.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {
    private final Map<String, SubscriptionPropertyValidator> propertyValidators = new HashMap<>();
    private final Map<String, String> propertyDefaultValueProviders = new HashMap<>();
    private final int NUMBER_OF_RETRIES_ALLOWED = 3;
    private final SubscriptionService subscriptionService;
    private final ProjectService projectService;
    private final VaradhiTopicService topicService;
    private final VaradhiSubscriptionFactory varadhiSubscriptionFactory;

    public SubscriptionHandlers(
            SubscriptionService subscriptionService, ProjectService projectService,
            VaradhiTopicService topicService, VaradhiSubscriptionFactory subscriptionFactory,
            RestOptions restOptions
    ) {
        this.subscriptionService = subscriptionService;
        this.projectService = projectService;
        this.topicService = topicService;
        this.varadhiSubscriptionFactory = subscriptionFactory;
        this.propertyValidators.putAll(SubscriptionPropertyValidator.createPropertyValidators(restOptions));
        this.propertyDefaultValueProviders.putAll(
                SubscriptionPropertyValidator.createPropertyDefaultValueProviders(restOptions));
    }

    public static String getSubscriptionFqn(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(PATH_PARAM_SUBSCRIPTION);
        return SubscriptionResource.buildInternalName(projectName, subscriptionName);
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/subscriptions",
                List.of(
                        RouteDefinition
                                .get("ListSubscriptions", "")
                                .authorize(SUBSCRIPTION_LIST)
                                .build(this::getHierarchies, this::list),
                        RouteDefinition
                                .get("GetSubscription", "/:subscription")
                                .authorize(SUBSCRIPTION_GET)
                                .build(this::getHierarchies, this::get),
                        RouteDefinition
                                .post("CreateSubscription", "")
                                .hasBody()
                                .bodyParser(this::setSubscription)
                                .authorize(SUBSCRIPTION_CREATE)
                                .authorize(TOPIC_CONSUME)
                                .build(this::getHierarchies, this::create),
                        RouteDefinition
                                .put("UpdateSubscription", "/:subscription")
                                .nonBlocking()
                                .hasBody()
                                .bodyParser(this::setSubscription)
                                .authorize(SUBSCRIPTION_UPDATE)
                                .authorize(TOPIC_CONSUME)
                                .build(this::getHierarchies, this::update),
                        RouteDefinition
                                .delete("DeleteSubscription", "/:subscription")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_DELETE)
                                .build(this::getHierarchies, this::delete),
                        RouteDefinition
                                .post("StartSubscription", "/:subscription/start")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchies, this::start),
                        RouteDefinition.post("StopSubscription", "/:subscription/stop")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchies, this::stop)
                )
        ).get();
    }

    public void setSubscription(RoutingContext ctx) {
        SubscriptionResource subscriptionResource = ctx.body().asValidatedPojo(SubscriptionResource.class);
        ctx.put(CONTEXT_KEY_BODY, subscriptionResource);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project subscriptionProject = projectService.getCachedProject(ctx.request().getParam(PATH_PARAM_PROJECT));
        if (hasBody) {
            SubscriptionResource subscriptionResource = ctx.get(CONTEXT_KEY_BODY);
            Project topicProject = projectService.getProject(subscriptionResource.getTopicProject());
            return Map.ofEntries(Map.entry(
                    ResourceType.SUBSCRIPTION,
                    new SubscriptionHierarchy(subscriptionProject, subscriptionResource.getName())
            ), Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, subscriptionResource.getTopic())));
        }
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (null == subscriptionName) {
            return Map.of(ResourceType.PROJECT, new Hierarchies.ProjectHierarchy(subscriptionProject));
        }

        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        Project topicProject = projectService.getProject(topicNameSegments[0]);
        String topicName = topicNameSegments[1];
        return Map.ofEntries(
                Map.entry(ResourceType.SUBSCRIPTION, new SubscriptionHierarchy(subscriptionProject, subscriptionName)),
                Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, topicName))
        );
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        List<String> subscriptionNames = subscriptionService.getSubscriptionList(projectName);
        ctx.endApiWithResponse(subscriptionNames);
    }

    public void get(RoutingContext ctx) {
        String internalSubscriptionName = getSubscriptionFqn(ctx);
        SubscriptionResource subscription =
                SubscriptionResource.from(subscriptionService.getSubscription(internalSubscriptionName));
        ctx.endApiWithResponse(subscription);
    }

    public void create(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiTopic subscribedTopic = getSubscribedTopic(subscription);
        Project subProject = projectService.getCachedProject(subscription.getProject());
        VaradhiSubscription varadhiSubscription =
                varadhiSubscriptionFactory.get(subscription, subProject, subscribedTopic);
        VaradhiSubscription createdSubscription =
                subscriptionService.createSubscription(subscribedTopic, varadhiSubscription, subProject);
        ctx.endApiWithResponse(SubscriptionResource.from(createdSubscription));
    }

    public void update(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        //TODO::Evaluate separating these into individual update APIs.
        //Fix:: Update is allowed, though no change in the subscription, this can be avoided.
        ctx.handleResponse(subscriptionService.updateSubscription(subscription.getSubscriptionInternalName(),
                subscription.getVersion(),
                subscription.getDescription(), subscription.isGrouped(), subscription.getEndpoint(),
                subscription.getRetryPolicy(), subscription.getConsumptionPolicy(), ctx.getIdentityOrDefault()
        ).thenApply(SubscriptionResource::from));
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        Project subProject = projectService.getCachedProject(projectName);
        String deletedBy = ctx.getIdentityOrDefault();
        ctx.handleResponse(subscriptionService.deleteSubscription(getSubscriptionFqn(ctx), subProject, deletedBy));
    }

    public void start(RoutingContext ctx) {
        String startedBy = ctx.getIdentityOrDefault();
        ctx.handleResponse(subscriptionService.start(getSubscriptionFqn(ctx), startedBy));
    }

    public void stop(RoutingContext ctx) {
        String stoppedBy = ctx.getIdentityOrDefault();
        ctx.handleResponse(subscriptionService.stop(getSubscriptionFqn(ctx), stoppedBy));
    }

    private SubscriptionResource getValidSubscriptionResource(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        SubscriptionResource subscription = ctx.get(CONTEXT_KEY_BODY);

        boolean ignoreConstraints =
                ctx.queryParam("ignoreConstraints").stream().map(Boolean::parseBoolean).findFirst().orElse(false);
        boolean isSuperAdmin = ctx.isSuperUser();
        if (!isSuperAdmin && ignoreConstraints) {
            throw new HttpException(HTTP_UNAUTHORIZED, "ignoreConstraints is only allowed for super admins.");
        }

        // ensure project name consistent
        if (!projectName.equals(subscription.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        }
        ensureValidatedProperties(subscription.getProperties(), ignoreConstraints);
        validateRetryPolicy(subscription.getRetryPolicy());
        return subscription;
    }

    private void validateRetryPolicy(RetryPolicy retryPolicy) {
        if (retryPolicy.getRetryAttempts() != NUMBER_OF_RETRIES_ALLOWED) {
            throw new IllegalArgumentException(
                    String.format("Only %d retries are supported.", NUMBER_OF_RETRIES_ALLOWED));
        }
    }


    private VaradhiTopic getSubscribedTopic(SubscriptionResource subscription) {
        String projectName = subscription.getTopicProject();
        String topicResourceName = subscription.getTopic();
        String topicName = String.join(NAME_SEPARATOR, projectName, topicResourceName);
        return topicService.get(topicName);
    }


    // usePermissible -- avoids full validations (e.g max/min values) on property value, but validator
    // may still perform minimal validation e.g. if value is syntactically correct (valid integer, enum, string)
    private void ensureValidatedProperties(Map<String, String> properties, boolean usePermissible) {
        List<String> unsupported =
                properties.keySet().stream().filter(key -> !propertyValidators.containsKey(key)).toList();
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException("Unsupported properties found: " + String.join(", ", unsupported));
        }
        propertyDefaultValueProviders.forEach((propName, defaultValue) -> {
            if (!properties.containsKey(propName)) {
                properties.put(propName, defaultValue);
            }
        });

        propertyValidators.forEach((propName, propValidator) -> {
            String propertyValue = properties.get(propName);
            if (!propValidator.isValid(propertyValue, usePermissible)) {
                throw new IllegalArgumentException("Invalid value for property " + propName + ": " + propertyValue);
            }
        });
    }
}

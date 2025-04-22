package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_DELETION_TYPE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_IGNORE_CONSTRAINTS;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_MESSAGE;
import static com.flipkart.varadhi.entities.Hierarchies.SubscriptionHierarchy;
import static com.flipkart.varadhi.entities.Hierarchies.TopicHierarchy;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_CREATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_LIST;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_UPDATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_SUBSCRIBE;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * Handles various subscription-related operations such as listing, creating, updating, deleting, restoring,
 * starting, and stopping subscriptions.
 */
@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {
    private static final String API_NAME = "SUBSCRIPTION";

    private static final int NUMBER_OF_RETRIES_ALLOWED = 3;

    private final SubscriptionService subscriptionService;
    private final VaradhiTopicService topicService;
    private final VaradhiSubscriptionFactory varadhiSubscriptionFactory;
    private final Map<String, SubscriptionPropertyValidator> propertyValidators;
    private final Map<String, String> propertyDefaultValueProviders;
    private final EntityReadCache<Project> projectCache;

    /**
     * Constructs a new SubscriptionHandlers instance.
     *
     * @param subscriptionService the service to manage subscriptions
     * @param topicService        the service to manage topics
     * @param subscriptionFactory the factory to create subscriptions
     * @param restOptions         the REST options configuration
     * @param projectCache        the entity read cache for projects
     */
    public SubscriptionHandlers(
        SubscriptionService subscriptionService,
        VaradhiTopicService topicService,
        VaradhiSubscriptionFactory subscriptionFactory,
        RestOptions restOptions,
        EntityReadCache<Project> projectCache
    ) {
        this.subscriptionService = subscriptionService;
        this.topicService = topicService;
        this.varadhiSubscriptionFactory = subscriptionFactory;
        this.propertyValidators = SubscriptionPropertyValidator.createPropertyValidators(restOptions);
        this.propertyDefaultValueProviders = SubscriptionPropertyValidator.createPropertyDefaultValueProviders(
            restOptions
        );
        this.projectCache = projectCache;
    }

    /**
     * Returns the list of route definitions for subscription-related operations.
     *
     * @return the list of route definitions
     */
    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project/subscriptions",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "")
                               .authorize(SUBSCRIPTION_LIST)
                               .build(this::getHierarchies, this::list),
                RouteDefinition.get(GET, API_NAME, "/:subscription")
                               .authorize(SUBSCRIPTION_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setSubscription)
                               .authorize(SUBSCRIPTION_CREATE)
                               .authorize(TOPIC_SUBSCRIBE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.put(UPDATE, API_NAME, "/:subscription")
                               .nonBlocking()
                               .hasBody()
                               .bodyParser(this::setSubscription)
                               .authorize(SUBSCRIPTION_UPDATE)
                               .authorize(TOPIC_SUBSCRIBE)
                               .build(this::getHierarchies, this::update),
                RouteDefinition.delete(DELETE, API_NAME, "/:subscription")
                               .nonBlocking()
                               .authorize(SUBSCRIPTION_DELETE)
                               .build(this::getHierarchies, this::delete),
                RouteDefinition.patch(RESTORE, API_NAME, "/:subscription/restore")
                               .nonBlocking()
                               .authorize(SUBSCRIPTION_UPDATE)
                               .build(this::getHierarchies, this::restore),
                RouteDefinition.post(START, API_NAME, "/:subscription/start")
                               .nonBlocking()
                               .authorize(SUBSCRIPTION_UPDATE)
                               .build(this::getHierarchies, this::start),
                RouteDefinition.post(STOP, API_NAME, "/:subscription/stop")
                               .nonBlocking()
                               .authorize(SUBSCRIPTION_UPDATE)
                               .build(this::getHierarchies, this::stop)
            )
        ).get();
    }

    /**
     * Parses and sets the subscription resource from the request body.
     *
     * @param ctx the routing context
     */
    public void setSubscription(RoutingContext ctx) {
        SubscriptionResource subscriptionResource = ctx.body().asValidatedPojo(SubscriptionResource.class);
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActorCode actorCode = isVaradhiAdmin(requestedBy) ?
            LifecycleStatus.ActorCode.ADMIN_ACTION :
            LifecycleStatus.ActorCode.USER_ACTION;
        subscriptionResource.setActorCode(actorCode);
        ctx.put(REQUEST_BODY, subscriptionResource);
    }

    /**
     * Retrieves the resource hierarchies for the given context.
     *
     * @param ctx     the routing context
     * @param hasBody whether the request has a body
     *
     * @return the map of resource types to their hierarchies
     */
    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project subscriptionProject = projectCache.getOrThrow(ctx.request().getParam(PATH_PARAM_PROJECT));
        if (hasBody) {
            SubscriptionResource subscriptionResource = ctx.get(REQUEST_BODY);
            Project topicProject = projectService.getProject(subscriptionResource.getTopicProject());

            return Map.ofEntries(
                Map.entry(
                    ResourceType.SUBSCRIPTION,
                    new SubscriptionHierarchy(subscriptionProject, subscriptionResource.getName())
                ),
                Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, subscriptionResource.getTopic()))
            );
        }
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (subscriptionName == null) {
            return Map.of(ResourceType.PROJECT, new Hierarchies.ProjectHierarchy(subscriptionProject));
        }

        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        Project topicProject = projectCache.getOrThrow(topicNameSegments[0]);
        String topicName = topicNameSegments[1];

        return Map.ofEntries(
            Map.entry(ResourceType.SUBSCRIPTION, new SubscriptionHierarchy(subscriptionProject, subscriptionName)),
            Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, topicName))
        );
    }

    /**
     * Lists all subscriptions for a given project.
     *
     * @param ctx the routing context
     *            - includeInactive: query parameter to include inactive or soft-deleted subscriptions
     */
    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        boolean includeInactive = ctx.queryParam(QUERY_PARAM_INCLUDE_INACTIVE)
                                     .stream()
                                     .findFirst()
                                     .map(Boolean::parseBoolean)
                                     .orElse(false);

        List<String> subscriptionNames = subscriptionService.getSubscriptionList(projectName, includeInactive);
        ctx.endApiWithResponse(subscriptionNames);
    }

    /**
     * Retrieves a specific subscription.
     *
     * @param ctx the routing context
     */
    public void get(RoutingContext ctx) {
        String internalSubscriptionName = getSubscriptionFqn(ctx);
        SubscriptionResource subscription = SubscriptionResource.from(
            subscriptionService.getSubscription(internalSubscriptionName)
        );
        ctx.endApiWithResponse(subscription);
    }

    /**
     * Creates a new subscription.
     *
     * @param ctx the routing context
     */
    public void create(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiTopic subscribedTopic = getSubscribedTopic(subscription);
        Project subProject = projectCache.getOrThrow(subscription.getProject());

        VaradhiSubscription varadhiSubscription = varadhiSubscriptionFactory.get(
            subscription,
            subProject,
            subscribedTopic
        );
        VaradhiSubscription createdSubscription = subscriptionService.createSubscription(
            subscribedTopic,
            varadhiSubscription,
            subProject
        );

        ctx.endApiWithResponse(SubscriptionResource.from(createdSubscription));
    }

    /**
     * Updates an existing subscription.
     *
     * @param ctx the routing context
     */
    public void update(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        // TODO: Consider splitting these into separate update APIs.
        // Note: Updates are currently allowed even if there are no changes to the subscription, which should be avoided.
        ctx.handleResponse(
            subscriptionService.updateSubscription(
                subscription.getSubscriptionInternalName(),
                subscription.getVersion(),
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getRetryPolicy(),
                subscription.getConsumptionPolicy(),
                ctx.getIdentityOrDefault()
            ).thenApply(SubscriptionResource::from)
        );
    }

    /**
     * Deletes a subscription.
     *
     * @param ctx the routing context
     */
    public void delete(RoutingContext ctx) {
        ResourceDeletionType deletionType = ctx.queryParam(QUERY_PARAM_DELETION_TYPE)
                                               .stream()
                                               .map(ResourceDeletionType::fromValue)
                                               .findFirst()
                                               .orElse(ResourceDeletionType.SOFT_DELETE);
        ResourceActionRequest actionRequest = createResourceActionRequest(ctx);

        ctx.handleResponse(
            subscriptionService.deleteSubscription(
                getSubscriptionFqn(ctx),
                projectCache.getOrThrow(ctx.pathParam(PATH_PARAM_PROJECT)),
                ctx.getIdentityOrDefault(),
                deletionType,
                actionRequest
            )
        );
    }

    /**
     * Restores a deleted subscription.
     *
     * @param ctx the routing context
     */
    public void restore(RoutingContext ctx) {
        ResourceActionRequest actionRequest = createResourceActionRequest(ctx);

        ctx.handleResponse(
            subscriptionService.restoreSubscription(getSubscriptionFqn(ctx), ctx.getIdentityOrDefault(), actionRequest)
        );
    }

    /**
     * Starts a subscription.
     *
     * @param ctx the routing context
     */
    public void start(RoutingContext ctx) {
        ctx.handleResponse(subscriptionService.start(getSubscriptionFqn(ctx), ctx.getIdentityOrDefault()));
    }

    /**
     * Stops a subscription.
     *
     * @param ctx the routing context
     */
    public void stop(RoutingContext ctx) {
        ctx.handleResponse(subscriptionService.stop(getSubscriptionFqn(ctx), ctx.getIdentityOrDefault()));
    }

    /**
     * Constructs the fully qualified name of a subscription.
     *
     * @param ctx the routing context
     *
     * @return the fully qualified name of the subscription
     */
    public static String getSubscriptionFqn(RoutingContext ctx) {
        return SubscriptionResource.buildInternalName(
            ctx.pathParam(PATH_PARAM_PROJECT),
            ctx.pathParam(PATH_PARAM_SUBSCRIPTION)
        );
    }

    /**
     * Validates and retrieves the subscription resource from the context.
     *
     * @param ctx the routing context
     *
     * @return the validated subscription resource
     */
    private SubscriptionResource getValidSubscriptionResource(RoutingContext ctx) {
        SubscriptionResource subscription = ctx.get(REQUEST_BODY);

        boolean ignoreConstraints = ignoreConstraints(ctx);
        validateSuperUserConstraints(ctx, ignoreConstraints);
        validateProjectConsistency(ctx.pathParam(PATH_PARAM_PROJECT), subscription.getProject());
        validateProperties(subscription.getProperties(), ignoreConstraints);
        validateRetryPolicy(subscription.getRetryPolicy());

        return subscription;
    }

    /**
     * Validates super user constraints.
     *
     * @param ctx               the routing context
     * @param ignoreConstraints whether to ignore constraints
     */
    private void validateSuperUserConstraints(RoutingContext ctx, boolean ignoreConstraints) {
        if (ignoreConstraints && !ctx.isSuperUser()) {
            throw new HttpException(HTTP_UNAUTHORIZED, "ignoreConstraints is restricted to super admins only.");
        }
    }

    /**
     * Validates the retry policy.
     *
     * @param retryPolicy the retry policy to validate
     */
    private void validateRetryPolicy(RetryPolicy retryPolicy) {
        if (retryPolicy.getRetryAttempts() != NUMBER_OF_RETRIES_ALLOWED) {
            throw new IllegalArgumentException(
                String.format("Only %d retries are supported.", NUMBER_OF_RETRIES_ALLOWED)
            );
        }
    }

    /**
     * Validates the consistency of the project name between the URL and the request body.
     *
     * @param projectPath      the project name from the URL
     * @param projectInRequest the project name from the request body
     */
    private void validateProjectConsistency(String projectPath, String projectInRequest) {
        if (!projectPath.equals(projectInRequest)) {
            throw new IllegalArgumentException("Project name mismatch between URL and request body.");
        }
    }

    /**
     * Retrieves the subscribed topic for a given subscription.
     *
     * @param subscription the subscription resource
     *
     * @return the subscribed topic
     */
    private VaradhiTopic getSubscribedTopic(SubscriptionResource subscription) {
        String topicName = String.join(NAME_SEPARATOR, subscription.getTopicProject(), subscription.getTopic());
        return topicService.get(topicName);
    }

    /**
     * Determines whether to ignore constraints based on the query parameters.
     *
     * @param ctx the routing context
     *
     * @return true if constraints should be ignored, false otherwise
     */
    private boolean ignoreConstraints(RoutingContext ctx) {
        return ctx.queryParam(QUERY_PARAM_IGNORE_CONSTRAINTS)
                  .stream()
                  .map(Boolean::parseBoolean)
                  .findFirst()
                  .orElse(false);
    }

    /**
     * Validates the properties of a subscription.
     *
     * @param properties     the properties to validate
     * @param usePermissible if true, skips full validations (e.g., max/min values) on property values,
     *                       but the validator may still perform minimal validation, such as checking if
     *                       the value is syntactically correct (valid integer, enum, string)
     *
     * @throws IllegalArgumentException if there are unsupported properties or invalid values
     */
    private void validateProperties(Map<String, String> properties, boolean usePermissible) {
        List<String> unsupportedKeys = properties.keySet()
                                                 .stream()
                                                 .filter(key -> !propertyValidators.containsKey(key))
                                                 .toList();

        if (!unsupportedKeys.isEmpty()) {
            throw new IllegalArgumentException("Unsupported properties: " + String.join(", ", unsupportedKeys));
        }

        propertyDefaultValueProviders.forEach((propName, defaultValue) -> {
            if (!properties.containsKey(propName)) {
                properties.put(propName, defaultValue);
            }
        });

        propertyValidators.forEach((key, validator) -> {
            String value = properties.get(key);
            if (!validator.isValid(value, usePermissible)) {
                throw new IllegalArgumentException("Invalid value for property: " + key);
            }
        });
    }

    /**
     * Creates a resource action request from the routing context.
     *
     * @param ctx the routing context
     *
     * @return the resource action request
     */
    private ResourceActionRequest createResourceActionRequest(RoutingContext ctx) {
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActorCode actorCode = isVaradhiAdmin(requestedBy) ?
            LifecycleStatus.ActorCode.ADMIN_ACTION :
            LifecycleStatus.ActorCode.USER_ACTION;
        String message = ctx.queryParam(QUERY_PARAM_MESSAGE).stream().findFirst().orElse("");
        return new ResourceActionRequest(actorCode, message);
    }

    /**
     * Checks if the identity is a Varadhi admin.
     * TODO: Replace with a call to isVaradhiAdmin(requestedBy) when authorization is implemented.
     *
     * @param identity the identity to check
     *
     * @return true if the identity is a Varadhi admin, false otherwise
     */
    private boolean isVaradhiAdmin(String identity) {
        return Objects.equals(identity, "varadhi-admin");
    }
}

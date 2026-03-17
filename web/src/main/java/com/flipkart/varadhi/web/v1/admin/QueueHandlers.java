package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.RequestActionType;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.core.subscription.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.core.VaradhiSubscriptionService;
import com.flipkart.varadhi.core.topic.VaradhiTopicFactory;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.ProjectHierarchy;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.flipkart.varadhi.entities.web.*;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_QUEUE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_DELETION_TYPE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_MESSAGE;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_CREATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_LIST;
import static com.flipkart.varadhi.entities.auth.ResourceAction.SUBSCRIPTION_UPDATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_CREATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_LIST;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_SUBSCRIBE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_UPDATE;

/**
 * Handler for Queue CRUD and restore. A queue is implemented as a topic plus a default
 * queue-style subscription (subscription name = {@link QueueResource#getDefaultSubscriptionName(String)}).
 */
@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class QueueHandlers implements RouteProvider {
    private static final String API_NAME = "QUEUE";

    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;
    private final VaradhiSubscriptionService varadhiSubscriptionService;
    private final VaradhiSubscriptionFactory varadhiSubscriptionFactory;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    public QueueHandlers(
        VaradhiTopicFactory varadhiTopicFactory,
        VaradhiTopicService varadhiTopicService,
        VaradhiSubscriptionService varadhiSubscriptionService,
        VaradhiSubscriptionFactory varadhiSubscriptionFactory,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache
    ) {
        this.varadhiTopicFactory = varadhiTopicFactory;
        this.varadhiTopicService = varadhiTopicService;
        this.varadhiSubscriptionService = varadhiSubscriptionService;
        this.varadhiSubscriptionFactory = varadhiSubscriptionFactory;
        this.projectCache = projectCache;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project/queues",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "")
                    .authorize(TOPIC_LIST)
                    .authorize(SUBSCRIPTION_LIST)
                    .build(this::getHierarchies, this::list),
                RouteDefinition.get(GET, API_NAME, "/:queue")
                    .authorize(TOPIC_GET)
                    .authorize(SUBSCRIPTION_GET)
                    .build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                    .hasBody()
                    .bodyParser(this::setRequestBody)
                    .authorize(TOPIC_CREATE)
                    .authorize(SUBSCRIPTION_CREATE)
                    .authorize(TOPIC_SUBSCRIBE)
                    .build(this::getHierarchies, this::create),
                RouteDefinition.delete(DELETE, API_NAME, "/:queue")
                    .authorize(SUBSCRIPTION_DELETE)
                    .authorize(TOPIC_DELETE)
                    .build(this::getHierarchies, this::delete),
                RouteDefinition.patch(RESTORE, API_NAME, "/:queue/restore")
                    .authorize(SUBSCRIPTION_UPDATE)
                    .authorize(TOPIC_UPDATE)
                    .build(this::getHierarchies, this::restore)
            )
        ).get();
    }

    public void setRequestBody(RoutingContext ctx) {
        QueueResource body = ctx.body().asValidatedPojo(QueueResource.class);
        ctx.put(REQUEST_BODY, body);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectCache.getOrThrow(projectName).getEntity();

        String queueName = ctx.request().getParam(PATH_PARAM_QUEUE);
        if (queueName == null) {
            return Map.of(ResourceType.PROJECT, new ProjectHierarchy(project));
        }
        return Map.of(ResourceType.TOPIC, new TopicHierarchy(project, queueName));
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        boolean includeInactive = ctx.queryParam(QUERY_PARAM_INCLUDE_INACTIVE)
            .stream()
            .findFirst()
            .map(Boolean::parseBoolean)
            .orElse(false);

        List<String> topicNames = varadhiTopicService.getVaradhiTopics(projectName, includeInactive)
            .stream()
            .filter(topic -> topic.startsWith(projectName + NAME_SEPARATOR))
            .map(topic -> topic.split(NAME_SEPARATOR_REGEX)[1])
            .toList();

        java.util.Set<String> subscriptionNames = new java.util.HashSet<>(
            varadhiSubscriptionService.getSubscriptionList(projectName, includeInactive)
        );
        List<String> queues = topicNames.stream()
            .filter(topicName -> subscriptionNames.contains(
                subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(topicName))))
            .toList();

        ctx.endApiWithResponse(queues);
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String queueName = ctx.pathParam(PATH_PARAM_QUEUE);
        String topicFqn = topicFqn(projectName, queueName);
        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));

        VaradhiTopic topic = varadhiTopicService.get(topicFqn);
        var subscription = varadhiSubscriptionService.getSubscription(subFqn);

        QueueResponse response = new QueueResponse(
            queueName,
            projectName,
            com.flipkart.varadhi.entities.web.TopicResource.from(topic),
            com.flipkart.varadhi.entities.web.SubscriptionResource.from(subscription)
        );
        ctx.endApiWithResponse(response);
    }

    public void create(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        QueueResource queueResource = ctx.get(REQUEST_BODY);
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActionCode actionCode = isVaradhiAdmin(requestedBy)
            ? LifecycleStatus.ActionCode.ADMIN_ACTION
            : LifecycleStatus.ActionCode.USER_ACTION;

        validateProjectAndName(projectName, queueResource);

        queueResource.setName(queueResource.getName() != null ? queueResource.getName().trim() : "");
        Project project = projectCache.getOrThrow(projectName).getEntity();

        TopicResource topicResource =
            QueueRequestMapper.toTopicResource(queueResource, projectName, actionCode);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(varadhiTopic, project);

        SubscriptionResource subscriptionResource =
            QueueRequestMapper.toSubscriptionResource(queueResource, projectName, actionCode);
        VaradhiTopic createdTopic = varadhiTopicService.get(topicFqn(projectName, queueResource.getName()));
        var varadhiSubscription = varadhiSubscriptionFactory.get(subscriptionResource, project, createdTopic);
        var createdSubscription = varadhiSubscriptionService.createSubscription(createdTopic, varadhiSubscription, project);

        QueueResponse response = new QueueResponse(
            queueResource.getName(),
            projectName,
            com.flipkart.varadhi.entities.web.TopicResource.from(createdTopic),
            com.flipkart.varadhi.entities.web.SubscriptionResource.from(createdSubscription)
        );
        ctx.endApiWithResponse(response);
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String queueName = ctx.pathParam(PATH_PARAM_QUEUE);
        ResourceDeletionType deletionType = ctx.queryParam(QUERY_PARAM_DELETION_TYPE)
            .stream()
            .map(ResourceDeletionType::fromValue)
            .findFirst()
            .orElse(ResourceDeletionType.SOFT_DELETE);
        RequestActionType actionRequest = createResourceActionRequest(ctx);

        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));
        String topicFqn = topicFqn(projectName, queueName);

        varadhiSubscriptionService.deleteSubscription(
            subFqn,
            projectCache.getOrThrow(projectName).getEntity(),
            ctx.getIdentityOrDefault(),
            deletionType,
            actionRequest
        );
        varadhiTopicService.delete(topicFqn, deletionType, actionRequest);
        ctx.endApi();
    }

    public void restore(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String queueName = ctx.pathParam(PATH_PARAM_QUEUE);
        RequestActionType actionRequest = createResourceActionRequest(ctx);

        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));
        String topicFqn = topicFqn(projectName, queueName);

        varadhiSubscriptionService.restoreSubscription(subFqn, ctx.getIdentityOrDefault(), actionRequest);
        varadhiTopicService.restore(topicFqn, actionRequest);
        ctx.endApi();
    }

    private void validateProjectAndName(String projectName, QueueResource queue) {
        if (queue.getName() == null || queue.getName().isBlank()) {
            throw new IllegalArgumentException("Queue name is required.");
        }
        if (queue.getTeam() != null && !queue.getTeam().isBlank()) {
            // optional: validate team exists if needed
        }
    }

    private static String topicFqn(String project, String queueOrTopicName) {
        return String.join(NAME_SEPARATOR, project, queueOrTopicName);
    }

    private static String subscriptionFqn(String project, String subscriptionName) {
        return com.flipkart.varadhi.entities.web.SubscriptionResource.buildInternalName(project, subscriptionName);
    }

    private RequestActionType createResourceActionRequest(RoutingContext ctx) {
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActionCode actionCode = isVaradhiAdmin(requestedBy)
            ? LifecycleStatus.ActionCode.ADMIN_ACTION
            : LifecycleStatus.ActionCode.USER_ACTION;
        String message = ctx.queryParam(QUERY_PARAM_MESSAGE).stream().findFirst().orElse("");
        return new RequestActionType(actionCode, message);
    }

    private boolean isVaradhiAdmin(String identity) {
        return Objects.equals(identity, "varadhi-admin");
    }

    /**
     * Response DTO for GET queue: topic + default subscription.
     */
    public record QueueResponse(
        String queueName,
        String project,
        TopicResource topic,
        com.flipkart.varadhi.entities.web.SubscriptionResource subscription
    ) {}
}

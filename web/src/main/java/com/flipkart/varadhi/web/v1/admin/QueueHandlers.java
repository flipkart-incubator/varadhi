package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.RequestActionType;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.VaradhiQueueService;
import com.flipkart.varadhi.entities.Constants;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.ProjectHierarchy;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.SubscriptionHierarchy;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.VARADHI_ADMIN_IDENTITY;
import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_QUEUE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_DELETION_TYPE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_MESSAGE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

/**
 * Handler for queue CRUD, update, and restore. Delegates to {@link VaradhiQueueService}. Update follows
 * {@link SubscriptionHandlers#update(RoutingContext)} for the subscription leg; {@code version} is the subscription
 * version for optimistic locking. The topic leg of the body is persisted before the subscription update
 * (see {@link VaradhiQueueService#updateQueue}). {@link #setRequestBody} fills omitted subscription properties,
 * retry policy, and consumption policy using {@link Constants.QueueDefaults} (same values as former {@code QueueResource} defaults).
 */
@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class})
public class QueueHandlers implements RouteProvider {
    private static final String API_NAME = "QUEUE";

    private final VaradhiQueueService varadhiQueueService;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    public QueueHandlers(
        VaradhiQueueService varadhiQueueService,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache
    ) {
        this.varadhiQueueService = varadhiQueueService;
        this.projectCache = projectCache;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project/queues",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "").authorize(TOPIC_LIST).build(this::getHierarchies, this::list),
                RouteDefinition.get(GET, API_NAME, "/:queue")
                               .authorize(TOPIC_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setRequestBody)
                               .authorize(TOPIC_CREATE)
                               .authorize(TOPIC_SUBSCRIBE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.put(UPDATE, API_NAME, "/:queue")
                               .nonBlocking()
                               .hasBody()
                               .bodyParser(this::setRequestBody)
                               .authorize(TOPIC_UPDATE)
                               .build(this::getHierarchies, this::update),
                RouteDefinition.delete(DELETE, API_NAME, "/:queue")
                               .nonBlocking()
                               .authorize(TOPIC_DELETE)
                               .build(this::getHierarchies, this::delete),
                RouteDefinition.patch(RESTORE, API_NAME, "/:queue/restore")
                               .nonBlocking()
                               .authorize(TOPIC_UPDATE)
                               .build(this::getHierarchies, this::restore)
            )
        ).get();
    }

    public void setRequestBody(RoutingContext ctx) {
        QueueResource body = ctx.body().asValidatedPojo(QueueResource.class);
        String name = body.getName();
        if (name != null) {
            body.setName(name.trim());
        }
        if (body.getProperties() == null || body.getProperties().isEmpty()) {
            body.setProperties(Map.copyOf(Constants.QueueDefaults.SUBSCRIPTION_PROPERTIES));
        }
        if (body.getRetryPolicy() == null) {
            body.setRetryPolicy(Constants.QueueDefaults.RETRY_POLICY);
        }
        if (body.getConsumptionPolicy() == null) {
            body.setConsumptionPolicy(Constants.QueueDefaults.CONSUMPTION_POLICY);
        }
        ctx.put(REQUEST_BODY, body);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectCache.getOrThrow(projectName).getEntity();

        if (hasBody) {
            QueueResource queueResource = ctx.get(REQUEST_BODY);
            String queueNameFromPath = ctx.pathParam(PATH_PARAM_QUEUE);
            String effectiveQueueName = (queueNameFromPath != null && !queueNameFromPath.isBlank()) ?
                queueNameFromPath :
                (queueResource.getName() != null ? queueResource.getName() : "");
            return Map.ofEntries(
                Map.entry(ResourceType.TOPIC, new TopicHierarchy(project, effectiveQueueName)),
                Map.entry(
                    ResourceType.SUBSCRIPTION,
                    new SubscriptionHierarchy(project, QueueResource.getDefaultSubscriptionName(effectiveQueueName))
                )
            );
        }

        String queueName = ctx.request().getParam(PATH_PARAM_QUEUE);
        if (queueName == null) {
            return Map.of(ResourceType.PROJECT, new ProjectHierarchy(project));
        }
        return Map.ofEntries(
            Map.entry(ResourceType.TOPIC, new TopicHierarchy(project, queueName)),
            Map.entry(
                ResourceType.SUBSCRIPTION,
                new SubscriptionHierarchy(project, QueueResource.getDefaultSubscriptionName(queueName))
            )
        );
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        boolean includeInactive = ctx.queryParam(QUERY_PARAM_INCLUDE_INACTIVE)
                                     .stream()
                                     .findFirst()
                                     .map(Boolean::parseBoolean)
                                     .orElse(false);

        List<String> queues = varadhiQueueService.list(projectName, includeInactive);
        ctx.endApiWithResponse(queues);
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String queueName = ctx.pathParam(PATH_PARAM_QUEUE);

        VaradhiQueueService.QueueResult result = varadhiQueueService.get(projectName, queueName);
        QueueResponse response = new QueueResponse(
            queueName,
            projectName,
            TopicResource.from(result.topic()),
            SubscriptionResource.from(result.subscription())
        );
        ctx.endApiWithResponse(response);
    }

    public void create(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        QueueResource queueResource = ctx.get(REQUEST_BODY);
        LifecycleStatus.ActionCode actionCode = getActionCode(ctx);
        Project project = projectCache.getOrThrow(projectName).getEntity();
        VaradhiQueueService.QueueResult result = varadhiQueueService.create(queueResource, project, actionCode);
        QueueResponse response = new QueueResponse(
            queueResource.getName(),
            projectName,
            TopicResource.from(result.topic()),
            SubscriptionResource.from(result.subscription())
        );
        ctx.endApiWithResponse(response);
    }

    /**
     * Updates a queue (default subscription fields). Same semantics as {@link SubscriptionHandlers#update(RoutingContext)}.
     */
    public void update(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String queueName = ctx.pathParam(PATH_PARAM_QUEUE);
        QueueResource queueResource = ctx.get(REQUEST_BODY);
        validateProjectConsistency(projectName, queueResource.getProject());
        if (!queueName.equals(queueResource.getName())) {
            throw new IllegalArgumentException("Queue name in path must match request body name.");
        }
        LifecycleStatus.ActionCode actionCode = getActionCode(ctx);
        String requestedBy = ctx.getIdentityOrDefault();

        ctx.handleResponse(
            varadhiQueueService.updateQueue(projectName, queueName, queueResource, requestedBy, actionCode)
                               .thenApply(
                                   result -> new QueueResponse(
                                       queueName,
                                       projectName,
                                       TopicResource.from(result.topic()),
                                       SubscriptionResource.from(result.subscription())
                                   )
                               )
        );
    }

    private static void validateProjectConsistency(String projectPath, String projectInRequest) {
        if (projectInRequest == null || !projectPath.equals(projectInRequest)) {
            throw new IllegalArgumentException("Project name mismatch between URL and request body.");
        }
    }

    /**
     * Deletes a queue. Async composition (subscription then topic) is entirely inside
     * {@link VaradhiQueueService#deleteQueue}; this method only forwards the resulting future to the response helper.
     *
     * @param ctx the routing context
     */
    public void delete(RoutingContext ctx) {
        ResourceDeletionType deletionType = ctx.queryParam(QUERY_PARAM_DELETION_TYPE)
                                               .stream()
                                               .map(ResourceDeletionType::fromValue)
                                               .findFirst()
                                               .orElse(ResourceDeletionType.SOFT_DELETE);
        RequestActionType actionRequest = createResourceActionRequest(ctx);

        String projectName = getProjectName(ctx);
        String queueName = getQueueName(ctx);
        Project project = projectCache.getOrThrow(projectName).getEntity();
        String requestedBy = ctx.getIdentityOrDefault();

        ctx.handleResponse(
            varadhiQueueService.deleteQueue(projectName, queueName, project, requestedBy, deletionType, actionRequest)
        );
    }

    /**
     * Restores a queue. Async composition lives in {@link VaradhiQueueService#restoreQueue}.
     *
     * @param ctx the routing context
     */
    public void restore(RoutingContext ctx) {
        RequestActionType actionRequest = createResourceActionRequest(ctx);
        String projectName = getProjectName(ctx);
        String queueName = getQueueName(ctx);
        String requestedBy = ctx.getIdentityOrDefault();

        ctx.handleResponse(varadhiQueueService.restoreQueue(projectName, queueName, requestedBy, actionRequest));
    }

    /**
     * Project name from the route {@code :project} segment.
     */
    private String getProjectName(RoutingContext ctx) {
        return ctx.pathParam(PATH_PARAM_PROJECT);
    }

    /**
     * Queue name from the route {@code :queue} segment.
     */
    private String getQueueName(RoutingContext ctx) {
        return ctx.pathParam(PATH_PARAM_QUEUE);
    }

    private LifecycleStatus.ActionCode getActionCode(RoutingContext ctx) {
        String requestedBy = ctx.getIdentityOrDefault();
        return isVaradhiAdmin(requestedBy) ?
            LifecycleStatus.ActionCode.ADMIN_ACTION :
            LifecycleStatus.ActionCode.USER_ACTION;
    }

    private RequestActionType createResourceActionRequest(RoutingContext ctx) {
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActionCode actionCode = getActionCode(ctx);
        String message = ctx.queryParam(QUERY_PARAM_MESSAGE).stream().findFirst().orElse("");
        return new RequestActionType(actionCode, message);
    }

    private boolean isVaradhiAdmin(String identity) {
        return Objects.equals(identity, VARADHI_ADMIN_IDENTITY);
    }

    /**
     * Response DTO for GET queue: topic + default subscription.
     */
    public record QueueResponse(
        String queueName,
        String project,
        TopicResource topic,
        SubscriptionResource subscription
    ) {
    }
}

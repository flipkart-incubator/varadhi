package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.DlqService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.web.entities.DlqMessagesResponse;
import com.flipkart.varadhi.web.entities.DlqPageMarker;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.entities.Constants.SubscriptionProperties.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static com.flipkart.varadhi.entities.Hierarchies.*;
import static com.flipkart.varadhi.web.v1.admin.SubscriptionHandlers.getSubscriptionFqn;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class DlqHandlers implements RouteProvider {
    private static final long UNSPECIFIED_TS = 0L;
    private final SubscriptionService subscriptionService;
    private final ProjectService projectService;
    private final DlqService dlqService;

    public DlqHandlers(DlqService dlqService, SubscriptionService subscriptionService, ProjectService projectService) {
        this.dlqService = dlqService;
        this.subscriptionService = subscriptionService;
        this.projectService = projectService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/subscriptions/:subscription/dlq/messages",
                List.of(
                        RouteDefinition
                                .post("Unsideline", "/unsideline")
                                .nonBlocking()
                                .hasBody()
                                .bodyParser(this::setUnsidelineRequest)
                                .authorize(SUBSCRIPTION_GET)
                                .authorize(TOPIC_CONSUME)
                                .build(this::getHierarchies, this::enqueueUnsideline),
                        RouteDefinition
                                .get("GetMessages", "")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_GET)
                                .authorize(TOPIC_CONSUME)
                                .build(this::getHierarchies, this::getMessages)
                )
        ).get();
    }

    public void setUnsidelineRequest(RoutingContext ctx) {
        UnsidelineRequest request = ctx.body().asPojo(UnsidelineRequest.class);
        ctx.put(CONTEXT_KEY_BODY, request);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectService.getCachedProject(projectName);
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        Project topicProject = projectService.getProject(topicNameSegments[0]);
        String topicName = topicNameSegments[1];
        return Map.ofEntries(
                Map.entry(ResourceType.SUBSCRIPTION, new SubscriptionHierarchy(project, subscriptionName)),
                Map.entry(ResourceType.TOPIC, new TopicHierarchy(topicProject, topicName))
        );
    }

    public void enqueueUnsideline(RoutingContext ctx) {
        UnsidelineRequest unsidelineRequest = ctx.get(CONTEXT_KEY_BODY);
        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        log.info("Unsideline requested for Subscription:{}", subscription.getName());

        int max_messages = subscription.getIntProperty(UNSIDELINE_API_MESSAGE_COUNT);
        int max_groups = subscription.getIntProperty(UNSIDELINE_API_GROUP_COUNT);
        unsidelineRequest.validate(max_groups, max_messages);
        if (!unsidelineRequest.getGroupIds().isEmpty() || !unsidelineRequest.getMessageIds().isEmpty()) {
            throw new IllegalArgumentException("Selective unsideline is not yet supported.");
        }
        ctx.handleResponse(dlqService.unsideline(subscription, unsidelineRequest, ctx.getIdentityOrDefault()));
    }

    public void getMessages(RoutingContext ctx) {
        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        String limitStr = ctx.request().getParam("limit");
        int limit = limitStr == null ? subscription.getIntProperty(GETMESSAGES_API_MESSAGES_LIMIT) :
                Integer.parseInt(limitStr);
        long earliestFailedAt =
                Long.parseLong(ctx.request().getParam("earliestFailedAt", String.valueOf(UNSPECIFIED_TS)));
        String nextPageParam = ctx.request().getParam("nextPage", "");
        DlqPageMarker dlqPageMarker = DlqPageMarker.fromString(nextPageParam);
        validateGetMessageCriteria(subscription, earliestFailedAt, dlqPageMarker, limit);
        ctx.handleChunkedResponse(
                (Function<Consumer<DlqMessagesResponse>, CompletableFuture<Void>>) responseWriter -> dlqService.getMessages(
                        subscription, earliestFailedAt, dlqPageMarker, limit, responseWriter));
    }

    private void validateGetMessageCriteria(
            VaradhiSubscription subscription, long earliestFailedAt, DlqPageMarker dlqPageMarker,
            int limit
    ) {
        if (earliestFailedAt == UNSPECIFIED_TS  && !dlqPageMarker.hasMarkers()) {
            throw new IllegalArgumentException("At least one get messages criteria needs to be specified.");
        }
        if (earliestFailedAt != UNSPECIFIED_TS && dlqPageMarker.hasMarkers()) {
            throw new IllegalArgumentException(
                    "Only one of the get messages criteria should be specified.");
        }
        int max_limit = subscription.getIntProperty(GETMESSAGES_API_MESSAGES_LIMIT);
        if (limit > max_limit) {
            throw new IllegalArgumentException("Limit cannot be more than " + max_limit + ".");
        }
    }

}

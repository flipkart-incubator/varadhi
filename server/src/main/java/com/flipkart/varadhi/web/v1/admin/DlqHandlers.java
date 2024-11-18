package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.DlqService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.web.Extensions;
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
                                .post("GetMessages", "")
                                .nonBlocking()
                                .bodyParser(this::setGetMessageRequest)
                                .authorize(SUBSCRIPTION_GET)
                                .authorize(TOPIC_CONSUME)
                                .build(this::getHierarchies, this::getMessages)
                )
        ).get();
    }

    public void setGetMessageRequest(RoutingContext ctx) {
        GetMessagesRequest request = ctx.body().asPojo(GetMessagesRequest.class);
        ctx.put(CONTEXT_KEY_BODY, request);
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

    /*
     * To be discussed:
     * GetMessages is a POST call.
     * For selective message retrieval it needs list of messages references. For now these references are assumed
     * to be respective message offset. Encoding the list of offsets in query param could be challenging as offset
     * is an object.
     * To circumvent the above, GetMessages() is implemented as POST call instead of standard GET.
     * An alternative could be
     *  - still use GET.
     *  - use metastore offset (but metastore is optional)
     */
    public void getMessages(RoutingContext ctx) {
        GetMessagesRequest messagesRequest = ctx.get(CONTEXT_KEY_BODY);
        VaradhiSubscription subscription = subscriptionService.getSubscription(getSubscriptionFqn(ctx));
        ctx.handleChunkedResponse((Function<Consumer<GetMessagesResponse>, CompletableFuture<Void>>) responseWriter -> {
            int max_limit = subscription.getIntProperty(GETMESSAGES_API_MESSAGES_LIMIT);
            messagesRequest.validate(max_limit);
            return dlqService.getMessages(subscription, messagesRequest, responseWriter);
        });
    }

}

package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {
    private final int NUMBER_OF_RETRIES_ALLOWED = 3;
    private final SubscriptionService subscriptionService;
    private final ProjectService projectService;
    private final VaradhiTopicService topicService;
    private final VaradhiSubscriptionFactory varadhiSubscriptionFactory;

    public SubscriptionHandlers(
            SubscriptionService subscriptionService, ProjectService projectService,
            VaradhiTopicService topicService, VaradhiSubscriptionFactory subscriptionFactory
    ) {
        this.subscriptionService = subscriptionService;
        this.projectService = projectService;
        this.topicService = topicService;
        this.varadhiSubscriptionFactory = subscriptionFactory;
    }

    public static String getSubscriptionName(RoutingContext ctx) {
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
                                .build(this::getHierarchy, this::list),
                        RouteDefinition
                                .get("GetSubscription", "/:subscription")
                                .authorize(SUBSCRIPTION_GET)
                                .build(this::getHierarchy, this::get),
                        RouteDefinition
                                .post("CreateSubscription", "")
                                .hasBody()
                                .bodyParser(this::setSubscription)
                                .authorize(SUBSCRIPTION_CREATE)
                                .build(this::getHierarchy, this::create),
                        RouteDefinition
                                .put("UpdateSubscription", "/:subscription")
                                .nonBlocking()
                                .hasBody()
                                .bodyParser(this::setSubscription)
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchy, this::update),
                        RouteDefinition
                                .delete("DeleteSubscription", "/:subscription")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_DELETE)
                                .build(this::getHierarchy, this::delete),
                        RouteDefinition
                                .post("StartSubscription", "/:subscription/start")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchy, this::start),
                        RouteDefinition.post("StopSubscription", "/:subscription/stop")
                                .nonBlocking()
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchy, this::stop)
                )
        ).get();
    }

    public void setSubscription(RoutingContext ctx) {
        SubscriptionResource subscriptionResource = ctx.body().asValidatedPojo(SubscriptionResource.class);
        ctx.put(CONTEXT_KEY_BODY, subscriptionResource);
    }

    public ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectService.getCachedProject(projectName);
        if (hasBody) {
            SubscriptionResource subscriptionResource = ctx.get(CONTEXT_KEY_BODY);
            return new Hierarchies.SubscriptionHierarchy(
                    project.getOrg(), project.getTeam(), project.getName(), subscriptionResource.getName());
        }
        String subscriptionName = ctx.request().getParam(PATH_PARAM_SUBSCRIPTION);
        if (null == subscriptionName) {
            return new Hierarchies.ProjectHierarchy(project.getOrg(), project.getTeam(), project.getName());
        }
        return new Hierarchies.SubscriptionHierarchy(
                project.getOrg(), project.getTeam(), project.getName(), subscriptionName);
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        List<String> subscriptionNames = subscriptionService.getSubscriptionList(projectName);
        ctx.endApiWithResponse(subscriptionNames);
    }

    public void get(RoutingContext ctx) {
        String internalSubscriptionName = getSubscriptionName(ctx);
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
        executeAsyncRequest(
                ctx, () -> subscriptionService.updateSubscription(subscription.getSubscriptionInternalName(),
                        subscription.getVersion(),
                        subscription.getDescription(), subscription.isGrouped(), subscription.getEndpoint(),
                        subscription.getRetryPolicy(), subscription.getConsumptionPolicy(), ctx.getIdentityOrDefault()
                ).thenApply(SubscriptionResource::from));
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        Project subProject = projectService.getCachedProject(projectName);
        executeAsyncRequest(
                ctx, () -> subscriptionService.deleteSubscription(getSubscriptionName(ctx), subProject,
                        ctx.getIdentityOrDefault()
                ));
    }

    public void start(RoutingContext ctx) {
        executeAsyncRequest(ctx, () -> subscriptionService.start(getSubscriptionName(ctx), ctx.getIdentityOrDefault()));
    }

    public void stop(RoutingContext ctx) {
        executeAsyncRequest(ctx, () -> subscriptionService.stop(getSubscriptionName(ctx), ctx.getIdentityOrDefault()));
    }

    private SubscriptionResource getValidSubscriptionResource(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        SubscriptionResource subscription = ctx.get(CONTEXT_KEY_BODY);

        // ensure project name consistent
        if (!projectName.equals(subscription.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        }
        validateRetryPolicy(subscription.getRetryPolicy());
        return subscription;
    }

    private void validateRetryPolicy(RetryPolicy retryPolicy) {
       if (retryPolicy.getRetryAttempts() != NUMBER_OF_RETRIES_ALLOWED) {
           throw new IllegalArgumentException(String.format("Only %d retries are supported.", NUMBER_OF_RETRIES_ALLOWED));
       }
    }


    private VaradhiTopic getSubscribedTopic(SubscriptionResource subscription) {
        String projectName = subscription.getTopicProject();
        String topicResourceName = subscription.getTopic();
        String topicName = String.join(NAME_SEPARATOR, projectName, topicResourceName);
        return topicService.get(topicName);
    }

    private <T> void executeAsyncRequest(RoutingContext ctx, Callable<CompletableFuture<T>> callable) {
        try {
            callable.call().whenComplete((t, error) -> ctx.vertx().runOnContext((Void) -> {
                if (error != null) {
                    ctx.endRequestWithException(unwrapExecutionException(error));
                } else {
                    if (null == t) {
                        ctx.endRequest();
                    } else {
                        ctx.endRequestWithResponse(t);
                    }
                }
            }));
        } catch (Exception e) {
            ctx.endRequestWithException(e);
        }
    }

    private Throwable unwrapExecutionException(Throwable t) {
        if (t instanceof ExecutionException) {
            return t.getCause();
        } else {
            return t;
        }
    }
}

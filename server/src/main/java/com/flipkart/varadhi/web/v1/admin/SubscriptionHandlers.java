package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.SubscriptionResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.utils.SubscriptionHelper;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {

    private final SubscriptionService subscriptionService;

    public SubscriptionHandlers(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/subscriptions",
                List.of(
                        RouteDefinition
                                .get("")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_LIST, "{project}"))
                                .build(this::list),
                        RouteDefinition
                                .get("/:subscription")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_GET, "{project}/{subscription}"))
                                .build(this::get),
                        RouteDefinition
                                .post("")
                                .blocking().hasBody()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_CREATE, "{project}"))
                                .build(this::create),
                        RouteDefinition
                                .put("/:subscription")
                                .blocking().hasBody()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                                .build(this::update),
                        RouteDefinition
                                .delete("/:subscription")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_DELETE, "{project}/{subscription}"))
                                .build(this::delete),
                        RouteDefinition
                                .post("/:subscription/start")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                                .build(this::start),
                        RouteDefinition
                                .post("/:subscription/stop")
                                .blocking()
                                .authenticatedWith(
                                        PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                                .build(this::stop)
                )
        ).get();
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        List<String> subscriptionNames = subscriptionService.getSubscriptionList(projectName);
        ctx.endApiWithResponse(subscriptionNames);
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION);
        String internalSubscriptionName = SubscriptionHelper.buildSubscriptionName(projectName, subscriptionName);
        SubscriptionResource subscription =
                SubscriptionHelper.toResource(subscriptionService.getSubscription(internalSubscriptionName));
        ctx.endApiWithResponse(subscription);
    }

    public void create(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiSubscription varadhiSubscription = SubscriptionHelper.fromResource(subscription, INITIAL_VERSION);
        VaradhiSubscription createdSubscription = subscriptionService.createSubscription(varadhiSubscription);
        ctx.endApiWithResponse(SubscriptionHelper.toResource(createdSubscription));
    }

    public void update(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiSubscription varadhiSubscription =
                SubscriptionHelper.fromResource(subscription, subscription.getVersion());
        VaradhiSubscription updatedSubscription = subscriptionService.updateSubscription(varadhiSubscription);
        ctx.endApiWithResponse(SubscriptionHelper.toResource(updatedSubscription));
    }

    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION);
        String internalSubscriptionName = SubscriptionHelper.buildSubscriptionName(projectName, subscriptionName);
        subscriptionService.deleteSubscription(internalSubscriptionName);
        ctx.endApi();
    }

    public void start(RoutingContext ctx) {
        ctx.todo();
    }

    public void stop(RoutingContext ctx) {
        ctx.todo();
    }

    private SubscriptionResource getValidSubscriptionResource(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        SubscriptionResource subscription = ctx.body().asValidatedPojo(SubscriptionResource.class);

        // ensure project name consistent
        if (!projectName.equals(subscription.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        }
        return subscription;
    }
}
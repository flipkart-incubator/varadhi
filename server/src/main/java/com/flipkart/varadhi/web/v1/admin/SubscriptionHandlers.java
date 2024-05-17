package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.TopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProjectService;
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

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {

    private final SubscriptionService subscriptionService;
    private final ProjectService projectService;
    private final TopicService<VaradhiTopic> topicService;

    public SubscriptionHandlers(SubscriptionService subscriptionService, ProjectService projectService, TopicService<VaradhiTopic> topicService) {
        this.subscriptionService = subscriptionService;
        this.projectService = projectService;
        this.topicService = topicService;
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
                                .hasBody()
                                .bodyParser(this::setSubscription)
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchy, this::update),
                        RouteDefinition
                                .delete("DeleteSubscription", "/:subscription")
                                .authorize(SUBSCRIPTION_DELETE)
                                .build(this::getHierarchy, this::delete),
                        RouteDefinition
                                .post("StartSubscription", "/:subscription/start")
                                .authorize(SUBSCRIPTION_UPDATE)
                                .build(this::getHierarchy, this::start),
                        RouteDefinition.post("StopSubscription", "/:subscription/stop")
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
        String internalSubscriptionName = SubscriptionHelper.buildSubscriptionName(ctx);
        SubscriptionResource subscription =
                SubscriptionHelper.toResource(subscriptionService.getSubscription(internalSubscriptionName));
        ctx.endApiWithResponse(subscription);
    }

    public void create(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiTopic subscribedTopic = getSubscribedTopic(subscription);
        VaradhiSubscription varadhiSubscription =
                SubscriptionHelper.fromResource(subscription, subscribedTopic, INITIAL_VERSION);
        VaradhiSubscription createdSubscription = subscriptionService.createSubscription(varadhiSubscription);
        ctx.endApiWithResponse(SubscriptionHelper.toResource(createdSubscription));
    }

    public void update(RoutingContext ctx) {
        SubscriptionResource subscription = getValidSubscriptionResource(ctx);
        VaradhiTopic subscribedTopic = getSubscribedTopic(subscription);
        VaradhiSubscription varadhiSubscription =
                SubscriptionHelper.fromResource(subscription, subscribedTopic, subscription.getVersion());
        VaradhiSubscription updatedSubscription = subscriptionService.updateSubscription(varadhiSubscription);
        ctx.endApiWithResponse(SubscriptionHelper.toResource(updatedSubscription));
    }

    public void delete(RoutingContext ctx) {
        subscriptionService.deleteSubscription(SubscriptionHelper.buildSubscriptionName(ctx));
        ctx.endApi();
    }

    public void start(RoutingContext ctx) {
        subscriptionService.start(SubscriptionHelper.buildSubscriptionName(ctx), ctx.getIdentityOrDefault());
    }

    public void stop(RoutingContext ctx) {
        subscriptionService.stop(SubscriptionHelper.buildSubscriptionName(ctx), ctx.getIdentityOrDefault());
    }

    private SubscriptionResource getValidSubscriptionResource(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        SubscriptionResource subscription = ctx.get(CONTEXT_KEY_BODY);

        // ensure project name consistent
        if (!projectName.equals(subscription.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        }
        return subscription;
    }

    private VaradhiTopic getSubscribedTopic(SubscriptionResource subscription) {
        String projectName = subscription.getTopicProject();
        String topicResourceName = subscription.getTopic();
        String topicName = String.join(NAME_SEPARATOR, projectName, topicResourceName);
        return topicService.get(topicName);
    }
}

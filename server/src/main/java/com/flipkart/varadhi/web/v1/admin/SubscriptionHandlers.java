package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_SUBSCRIPTION;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class SubscriptionHandlers implements RouteProvider {

    private final MetaStore metaStore;

    public SubscriptionHandlers(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/subscriptions",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::list,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_LIST, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:subscription",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_GET, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "/:subscription",
                                Set.of(authenticated, hasBody),
                                new LinkedHashSet<>(),
                                this::create,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_CREATE, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.PUT,
                                "/:subscription",
                                Set.of(authenticated, hasBody),
                                new LinkedHashSet<>(),
                                this::update,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:subscription",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_DELETE, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "/:subscription/start",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::start,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "/:subscription/stop",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::stop,
                                true,
                                Optional.of(PermissionAuthorization.of(SUBSCRIPTION_UPDATE, "{project}/{subscription}"))
                        )
                )
        ).get();
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        List<String> subscriptionNames = metaStore.getVaradhiSubscriptionNames(projectName);
        ctx.endApiWithResponse(subscriptionNames);
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION);
        VaradhiSubscription varadhiSubscription = metaStore.getVaradhiSubscription(subscriptionName, projectName);
        ctx.endApiWithResponse(SubscriptionResource.of(varadhiSubscription));
    }

    public void create(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION);

        // ensure project name consistent
        SubscriptionResource subscription = ctx.body().asValidatedPojo(SubscriptionResource.class);
        if (!projectName.equals(subscription.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        } else if (!subscriptionName.equals(subscription.getName())) {
            throw new IllegalArgumentException("Specified Subscription name is different from Subscription name in url");
        }

        // check project and topic exists
        Project project = metaStore.getProject(subscription.getProject());
        TopicResource topic = metaStore.getTopicResource(subscription.getTopic(), project.getName());
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException("Cannot create grouped Subscription as it's Topic(%s:%s) is not grouped".formatted(project.getName(), subscription.getTopic()));
        }

        // check for duplicate subscription
        if (metaStore.checkVaradhiSubscriptionExists(subscription.getName(), project.getName())) {
            throw new IllegalArgumentException("Subscription(%s:%s) already exists".formatted(project.getName(), subscription.getName()));
        }

        // create VaradhiSubscription entity
        VaradhiSubscription persistedSub = new VaradhiSubscription(
                subscription.getName(),
                VersionedEntity.INITIAL_VERSION,
                project.getName(),
                subscription.getTopic(),
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint()
        );

        // persist
        metaStore.createVaradhiSubscription(persistedSub);

        // Return versioned Subscription resource as response
        ctx.endApiWithResponse(SubscriptionResource.of(persistedSub));
    }

    public void update(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String subscriptionName = ctx.pathParam(REQUEST_PATH_PARAM_SUBSCRIPTION);
        SubscriptionResource subscriptionUpdate = ctx.body().asValidatedPojo(SubscriptionResource.class);

        // check subscription exist
        if (!metaStore.checkVaradhiSubscriptionExists(subscriptionUpdate.getName(), subscriptionUpdate.getProject())) {
            throw new IllegalArgumentException("Subscription(%s:%s) does not exist".formatted(projectName, subscriptionName));
        }

        VaradhiSubscription existingSubscription = metaStore.getVaradhiSubscription(subscriptionName, projectName);
        if (subscriptionUpdate.getVersion() != existingSubscription.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Subscription(%s) has been modified. Fetch latest and try again.", existingSubscription.getName()
            ));
        }

        // only allow description, grouped, endpoint
        if (!Objects.equals(subscriptionUpdate.getTopic(), existingSubscription.getTopic())) {
            throw new IllegalArgumentException("Cannot update Topic of Subscription(%s:%s)".formatted(projectName, subscriptionName));
        }

        // if set grouped to true, but topic is not grouped
        TopicResource topic = metaStore.getTopicResource(subscriptionUpdate.getTopic(), projectName);
        if (subscriptionUpdate.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException("Cannot update Subscription(%s:%s) to grouped as it's Topic(%s:%s) is not grouped".formatted(projectName, subscriptionName, projectName, subscriptionUpdate.getTopic()));
        }

        // update
        VaradhiSubscription updatedVaradhiSubscription = new VaradhiSubscription(
                existingSubscription.getName(),
                existingSubscription.getVersion() + 1,
                existingSubscription.getProject(),
                existingSubscription.getTopic(),
                subscriptionUpdate.getDescription(),
                subscriptionUpdate.isGrouped(),
                subscriptionUpdate.getEndpoint()
        );

        int updatedVersion = metaStore.updateVaradhiSubscription(updatedVaradhiSubscription);
        updatedVaradhiSubscription.setVersion(updatedVersion);

        ctx.endApiWithResponse(SubscriptionResource.of(updatedVaradhiSubscription));
    }

    public void delete(RoutingContext ctx) {
        ctx.todo();
    }

    public void start(RoutingContext ctx) {
        ctx.todo();
    }

    public void stop(RoutingContext ctx) {
        ctx.todo();
    }
}

package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.StorageTopicServiceFactory;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.HandlerUtil;
import com.flipkart.varadhi.web.RouteDefinition;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.auth.ResourceAction.*;
import static com.flipkart.varadhi.web.RouteDefinition.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.RouteDefinition.RouteBehaviour.hasBody;

@Slf4j
public class TopicHandlers implements RouteDefinition.Provider {

    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;

    public TopicHandlers(StorageTopicFactory<StorageTopic> topicFactory, StorageTopicServiceFactory<StorageTopic> serviceFactory) {
        this.varadhiTopicFactory = new VaradhiTopicFactory(topicFactory);
        this.varadhiTopicService = new VaradhiTopicService(serviceFactory);
    }

    @Override
    public List<RouteDefinition> get() {
        return new RouteDefinition.SubRoutes(
                "/v1/tenants/:tenant/topics",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET, "/:topic", Set.of(), this::get,
                                Optional.of(PermissionAuthorization.of(TOPIC_GET, "{tenant}/{topic}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST, "", Set.of(authenticated, hasBody), this::create,
                                Optional.of(PermissionAuthorization.of(TOPIC_CREATE, "{tenant}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE, "/:topic", Set.of(), this::delete,
                                Optional.of(PermissionAuthorization.of(TOPIC_DELETE, "{tenant}/{topic}"))
                        )
                )
        ).get();
    }

    public void get(RoutingContext ctx) {
        HandlerUtil.handleTodo(ctx);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Enable authn/authz for this flow.
        //TODO:: Consider using Vertx ValidationHandlers to validate the request body.
        TopicResource topicResource = ctx.body().asPojo(TopicResource.class);
        //TODO::This should move to async/future pattern. It is getting executed on event loop.
        //TODO::implement the storage persistence.
        VaradhiTopic vt = varadhiTopicFactory.get(topicResource);
        varadhiTopicService.create(vt);
        //TODO:: Implement return response.
        ctx.response().end();
    }

    public void delete(RoutingContext ctx) {
        HandlerUtil.handleTodo(ctx);
    }
}

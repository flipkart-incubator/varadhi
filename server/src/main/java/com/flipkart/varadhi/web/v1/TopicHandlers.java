package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.web.HandlerUtil;
import com.flipkart.varadhi.web.RouteDefinition;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.auth.ResourceAction.*;

@Slf4j
public class TopicHandlers implements RouteDefinition.Provider {

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
                                HttpMethod.POST, "", Set.of(), this::create,
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
        HandlerUtil.handleTodo(ctx);
    }


    public void delete(RoutingContext ctx) {
        HandlerUtil.handleTodo(ctx);
    }
}

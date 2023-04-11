package com.flipkart.varadhi.handlers.v1;

import com.flipkart.varadhi.RouteDefinition;
import com.flipkart.varadhi.handlers.HandlerUtil;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Set;

import static com.flipkart.varadhi.RouteDefinition.RouteBehaviour.open;

public class TopicHandlers implements RouteDefinition.Provider {

    @Override
    public List<RouteDefinition> get() {
        return HandlerUtil.withBasePath(
                "/v1/topics",
                List.of(
                        new RouteDefinition(HttpMethod.GET, "/:topic", Set.of(open), this::get),
                        new RouteDefinition(HttpMethod.POST, "", Set.of(), this::create),
                        new RouteDefinition(HttpMethod.DELETE, "/:topic", Set.of(), this::delete)
                )
        );
    }

    public void get(RoutingContext event) {
        HandlerUtil.handleTodo(event);
    }

    public void create(RoutingContext event) {
        HandlerUtil.handleTodo(event);
    }


    public void delete(RoutingContext event) {
        HandlerUtil.handleTodo(event);
    }
}

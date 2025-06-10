package com.flipkart.varadhi.web.core.routes;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public enum RouteBehaviour {
    telemetry(0), hasBody(100), parseBody(200), addHierarchy(300),

    authenticated(400), post_authentication(401), authorized(500);

    RouteBehaviour(int order) {
        this.order = order;
    }

    final int order;
}

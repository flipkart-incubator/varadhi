package com.flipkart.varadhi.web.routes;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public enum RouteBehaviour {
    telemetry(0),
    authenticated(100),
    hasBody(200),
    parseBody(300),
    addHierarchy(400),
    authorized(500);

    RouteBehaviour(int order) {
        this.order = order;
    }

    final int order;
}

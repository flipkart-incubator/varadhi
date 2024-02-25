package com.flipkart.varadhi.web.routes;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public enum RouteBehaviour {
    hasBody(100),
    authenticated(200);

    RouteBehaviour(int order) {
        this.order = order;
    }

    final int order;
}

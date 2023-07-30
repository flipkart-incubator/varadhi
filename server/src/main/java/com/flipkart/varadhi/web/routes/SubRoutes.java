package com.flipkart.varadhi.web.routes;

import java.util.List;
import java.util.stream.Collectors;

public record SubRoutes(String basePath, List<RouteDefinition> subRoutes) implements RouteProvider {
    @Override
    public List<RouteDefinition> get() {
        return
                subRoutes.stream()
                        .map(r -> new RouteDefinition(
                                r.method(),
                                basePath + r.path(),
                                r.behaviours(),
                                r.preHandlers(),
                                r.endReqHandler(),
                                r.blockingEndHandler(),
                                r.requiredAuthorization()
                        ))
                        .collect(Collectors.toList());
    }
}

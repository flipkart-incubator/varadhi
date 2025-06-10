package com.flipkart.varadhi.web.core.routes;

import java.util.List;
import java.util.stream.Collectors;

public record SubRoutes(String basePath, List<RouteDefinition> subRoutes) implements RouteProvider {
    @Override
    public List<RouteDefinition> get() {
        return subRoutes.stream()
                        .map(
                            r -> new RouteDefinition(
                                r.getMethodName(),
                                r.getApiSubject(),
                                r.getMethod(),
                                basePath + r.getPath(),
                                r.getBehaviours(),
                                r.getPreHandlers(),
                                r.getEndReqHandler(),
                                r.isBlockingEndHandler(),
                                r.getBodyParser(),
                                r.getHierarchyFunction(),
                                r.getAuthorizeOnActions(),
                                r.getTelemetryType()
                            )
                        )
                        .collect(Collectors.toList());
    }
}

package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;


@Slf4j
public class ProduceVerticle extends AbstractVerticle {

    private final List<RouteDefinition> apiRoutes;
    private final Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfiguratos;

    public ProduceVerticle(
            Router router,
            List<RouteDefinition> apiRoutes,
            Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfiguratos,
            FailureHandler failureHandler
    ) {
        this.apiRoutes = apiRoutes;
        this.routeBehaviourConfiguratos = routeBehaviourConfiguratos;
        configureProduceRoutes(router, apiRoutes, failureHandler, routeBehaviourConfiguratos);
    }

    private void configureProduceRoutes(
            Router router,
            List<RouteDefinition> apiRoutes,
            FailureHandler failureHandler,
            Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfiguratos
    ) {
        log.info("Configuring Produce routes.");
        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.method()).path(def.path());
            def.behaviours().forEach(behaviour -> {
                        RouteConfigurator behaviorProvider = routeBehaviourConfiguratos.getOrDefault(behaviour, null);
                        if (null != behaviorProvider) {
                            behaviorProvider.configure(route, def);
                        } else {
                            String errMsg = String.format("No RouteBehaviourProvider configured for %s.", behaviour);
                            log.error(errMsg);
                            throw new InvalidStateException(errMsg);
                        }
                        behaviorProvider.configure(route, def);
                    }
            );
            route.handler(def.handler());
            route.failureHandler(failureHandler);
        }
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
    }


    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

    }

}

package com.flipkart.varadhi.deployment;

import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.admin.OrgHandlers;
import com.flipkart.varadhi.web.v1.admin.ProjectHandlers;
import com.flipkart.varadhi.web.v1.admin.TeamHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Vertx;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FullDeploymentVerticleDeployer extends VerticleDeployer {

    private final OrgHandlers orgHandlers;
    private final TeamHandlers teamHandlers;
    private final ProjectHandlers projectHandlers;

    public FullDeploymentVerticleDeployer(
            Vertx vertx, AppConfiguration configuration,
            MessagingStackProvider messagingStackProvider, MetaStoreProvider metaStoreProvider,
            MessageChannel messageChannel, MeterRegistry meterRegistry, Tracer tracer
    ) {
        super(vertx, configuration, messagingStackProvider, metaStoreProvider, messageChannel, meterRegistry, tracer);
        this.orgHandlers = new OrgHandlers(this.orgService);
        this.teamHandlers = new TeamHandlers(this.teamService);
        this.projectHandlers = new ProjectHandlers(this.projectService);
    }

    @Override
    public List<RouteDefinition> getRouteDefinitions() {
        return Stream.of(
                        super.getRouteDefinitions(),
                        orgHandlers.get(),
                        teamHandlers.get(),
                        projectHandlers.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}

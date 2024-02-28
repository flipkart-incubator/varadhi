package com.flipkart.varadhi.config;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.components.ComponentKind;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;
import lombok.Getter;

import java.util.List;

@Getter
public class AppConfiguration {
    @NotEmpty
    List<ComponentKind> components;

    @NotNull
    private VertxOptions vertxOptions;
    @NotNull
    private DeploymentOptions verticleDeploymentOptions;

    @NotNull
    private HttpServerOptions httpServerOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;

    @NotNull
    private RestOptions restOptions;

    @NotNull
    private ProducerOptions producerOptions;
    @NotNull
    private MessagingStackOptions messagingStackOptions;

    @NotNull
    private MetaStoreOptions metaStoreOptions;

    @NotNull
    private FeatureFlags featureFlags;
}
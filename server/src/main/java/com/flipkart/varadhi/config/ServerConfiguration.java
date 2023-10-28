package com.flipkart.varadhi.config;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.auth.AuthorizationOptions;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class ServerConfiguration {

    @NotNull
    private VaradhiOptions varadhiOptions;

    @NotNull
    private VertxOptions vertxOptions;

    @NotNull
    private DeploymentOptions verticleDeploymentOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;

    @NotNull
    private MessagingStackOptions messagingStackOptions;

    @NotNull
    private MetaStoreOptions metaStoreOptions;

    @NotNull
    private HttpServerOptions httpServerOptions;

}

package com.flipkart.varadhi.configs;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import lombok.Getter;

@Getter
public class ServerConfiguration {
    private VertxOptions vertxOptions;
    private DeploymentOptions deploymentOptions;

    private boolean authenticationEnabled;

    private AuthOptions authentication;
}

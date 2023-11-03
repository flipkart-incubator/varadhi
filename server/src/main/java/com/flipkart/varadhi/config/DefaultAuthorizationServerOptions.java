package com.flipkart.varadhi.config;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpServerOptions;
import lombok.Getter;

@Getter
public class DefaultAuthorizationServerOptions {
    private DeploymentOptions verticleDeploymentOptions;
    private HttpServerOptions httpServerOptions;
}

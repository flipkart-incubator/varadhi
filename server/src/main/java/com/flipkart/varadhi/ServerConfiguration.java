package com.flipkart.varadhi;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.auth.AuthorizationOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import lombok.Getter;

@Getter
public class ServerConfiguration {

    private VertxOptions vertxOptions;

    private DeploymentOptions deploymentOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;
}

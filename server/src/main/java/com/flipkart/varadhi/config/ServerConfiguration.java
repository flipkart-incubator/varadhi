package com.flipkart.varadhi.config;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.auth.AuthorizationOptions;
import com.flipkart.varadhi.db.MetaStoreOptions;
import com.flipkart.varadhi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import lombok.Getter;

@Getter
public class ServerConfiguration {
    private VaradhiOptions varadhiOptions;
    private VertxOptions vertxOptions;

    private DeploymentOptions restVerticleDeploymentOptions;

    private DeploymentOptions produceVerticleDeploymentOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;

    private MessagingStackOptions messagingStackOptions;

    private MetaStoreOptions metaStoreOptions;

    private HttpServerOptions httpServerOptions;

}

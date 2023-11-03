package com.flipkart.varadhi.config;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import lombok.Getter;

@Getter
public class ServerConfiguration {
    private VertxOptions vertxOptions;
    private DeploymentOptions verticleDeploymentOptions;
    private HttpServerOptions httpServerOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;

    private boolean defaultAuthorizationServerEnabled;

    private DefaultAuthorizationServerOptions defaultAuthorizationServerOptions;

    private RestOptions restOptions;

    private ProducerOptions producerOptions;

    private MessagingStackOptions messagingStackOptions;

    private MetaStoreOptions metaStoreOptions;

}

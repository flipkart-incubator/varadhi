package com.flipkart.varadhi;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.auth.AuthorizationOptions;
import com.flipkart.varadhi.config.VaradhiDeploymentConfig;
import com.flipkart.varadhi.db.MetaStoreOptions;
import com.flipkart.varadhi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import lombok.Getter;

@Getter
public class ServerConfiguration {

    private VertxOptions vertxOptions;

    private DeploymentOptions restVerticleDeploymentOptions;

    private boolean authenticationEnabled;

    private AuthenticationOptions authentication;

    private boolean authorizationEnabled;

    private AuthorizationOptions authorization;

    private MessagingStackOptions messagingStackOptions;

    private MetaStoreOptions metaStoreOptions;

    private VaradhiDeploymentConfig varadhiDeploymentConfig;

}

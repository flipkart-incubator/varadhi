package com.flipkart.varadhi.config;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.cluster.NodeResources;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class ServerConfig {
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

    /**
     * zookeeper options. This is used to connect to zookeeper for managing node cluster.
     */
    @NotNull
    private ZookeeperConnectConfig zookeeperOptions;

    /**
     * Overridable configuration to configure the amount of cpu & nic bandwidth available for this node.
     * In the future, when this becomes auto-detected, this parameter will become optional.
     */
    @NotNull
    private NodeResources nodeResourcesOverride;
    
    /**
     * A unique node Id across the whole cluster
     */

    @NotNull
    private String nodeId;
}

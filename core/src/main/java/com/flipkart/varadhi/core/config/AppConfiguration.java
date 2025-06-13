package com.flipkart.varadhi.core.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.common.ZookeeperConnectConfig;
import com.flipkart.varadhi.entities.Validatable;

import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
@JsonIgnoreProperties (ignoreUnknown = true)
public class AppConfiguration implements Validatable {

    @NotBlank (message = "Deployed region must be specified")
    private String deployedRegion;

    /**
     * Overridable configuration to configure the amount of cpu & nic bandwidth available for this node.
     * In the future, when this becomes auto-detected, this parameter will become optional.
     */
    @NotNull
    @Valid
    private MemberConfig member;

    @NotNull
    private VertxOptions vertxOptions;

    @NotNull
    private DeploymentOptions verticleDeploymentOptions;

    /**
     * Refer Vertx DeliveryOptions for details, used for inter node communication.
     * Use it to configure delivery timeout (default 1000ms) and tracing options (default PROPAGATE)
     */
    private DeliveryConfig deliveryOptions;

    @NotNull
    private ProducerOptions producerOptions;

    @NotNull
    private MessagingStackOptions messagingStackOptions;

    @NotNull
    private MetaStoreOptions metaStoreOptions;

    /**
     * zookeeper options. This is used to connect to zookeeper for managing node cluster.
     */
    @NotNull
    private ZookeeperConnectConfig zookeeperOptions;

    @NotNull
    private FeatureFlags featureFlags;

    @NotNull
    @Valid
    private MessageConfiguration messageConfiguration;

    @NotNull
    private MetricsExporterOptions metricsExporterOptions;

    private boolean tracesEnabled = true;

    @Override
    public void validate() {
        Validatable.super.validate();
    }
}

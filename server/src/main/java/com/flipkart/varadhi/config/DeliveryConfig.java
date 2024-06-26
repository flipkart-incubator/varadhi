package com.flipkart.varadhi.config;

import io.vertx.core.tracing.TracingPolicy;
import lombok.Data;

/**
 * Workaround for vertx DeliveryOptions
 * This is used in place of {@link io.vertx.core.eventbus.DeliveryOptions} to configure delivery timeout
 * and tracing policy for Vertx eventBus in ClusterManager flow.
 * (de)serialzation of DeliveryOptions is broken because of "all" property which is used via MultiMap headers.
 * com.fasterxml.jackson.databind.exc.InvalidDefinitionException:
 *     Conflicting setter definitions for property "all": io.vertx.core.MultiMap#setAll(io.vertx.core.MultiMap)
 *     vs io.vertx.core.MultiMap#setAll(java.util.Map)
 * https://github.com/eclipse-vertx/vert.x/issues/3735
 */
@Data
public class DeliveryConfig {
    private int timeoutMs = 10000;
    private TracingPolicy tracingPolicy = TracingPolicy.PROPAGATE;
}

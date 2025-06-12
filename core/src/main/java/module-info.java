module com.flipkart.varadhi.core {
    requires static lombok;
    requires org.slf4j;
    requires com.fasterxml.jackson.annotation;
    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.entities;
    requires jakarta.validation;
    requires com.google.common;
    requires io.vertx.core;
    requires io.vertx.clustermanager.zookeeper;
    requires dev.failsafe.core;
    requires io.opentelemetry.api;
    requires micrometer.core;
    requires io.opentelemetry.sdk.common;
    requires io.opentelemetry.semconv;
    requires io.opentelemetry.sdk.trace;
    requires io.opentelemetry.sdk;
    requires io.opentelemetry.context;
    requires micrometer.registry.jmx;
    requires micrometer.registry.prometheus;
    requires micrometer.registry.otlp;
    requires io.vertx.tracing.opentelemetry;
    requires curator.framework;

    exports com.flipkart.varadhi.core;
    exports com.flipkart.varadhi.core.cluster;
    exports com.flipkart.varadhi.core.cluster.events;
    exports com.flipkart.varadhi.core.subscription.allocation;
    exports com.flipkart.varadhi.core.subscription;
    exports com.flipkart.varadhi.core.topic;
    exports com.flipkart.varadhi.core.cluster.controller;
    exports com.flipkart.varadhi.core.cluster.consumer;
    exports com.flipkart.varadhi.core.cluster.messages;
}

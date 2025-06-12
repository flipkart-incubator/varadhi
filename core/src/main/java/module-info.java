module com.flipkart.varadhi.core {
    requires static lombok;
    requires com.fasterxml.jackson.annotation;
    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.entities;
    requires jakarta.validation;
    requires com.google.common;
    requires transitive io.vertx.core;
    requires io.vertx.clustermanager.zookeeper;
    requires curator.framework;
    requires dev.failsafe.core;

    exports com.flipkart.varadhi.core;
    exports com.flipkart.varadhi.core.cluster;
    exports com.flipkart.varadhi.core.cluster.events;
    exports com.flipkart.varadhi.core.subscription.allocation;
    exports com.flipkart.varadhi.core.subscription;
    exports com.flipkart.varadhi.core.cluster.api;
    exports com.flipkart.varadhi.core.topic;
    exports com.flipkart.varadhi.core.cluster.controller;
    exports com.flipkart.varadhi.core.cluster.consumer;
}

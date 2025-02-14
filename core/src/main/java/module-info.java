module com.flipkart.varadhi.core {
    requires static lombok;
    requires com.fasterxml.jackson.annotation;
    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.entities;
    requires jakarta.validation;
    requires io.vertx.core;
    requires com.google.common;

    exports com.flipkart.varadhi.core.cluster;
    exports com.flipkart.varadhi.core.cluster.entities;
}

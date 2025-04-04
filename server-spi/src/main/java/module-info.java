module com.flipkart.varadhi.server.spi {
    requires io.vertx.core;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.entities;
    requires micrometer.core;
    requires io.vertx.web;
    requires static lombok;
    requires com.fasterxml.jackson.annotation;
    requires jakarta.validation;

    exports com.flipkart.varadhi.server.spi.authn;
    exports com.flipkart.varadhi.server.spi.authz;
    exports com.flipkart.varadhi.server.spi.utils;
    exports com.flipkart.varadhi.server.spi.vo;
    exports com.flipkart.varadhi.server.spi;
}

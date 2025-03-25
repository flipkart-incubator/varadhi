module com.flipkart.varadhi.spi {
    requires static lombok;
    requires com.fasterxml.jackson.databind;
    requires com.flipkart.varadhi.entities;
    requires jakarta.validation;
    requires io.vertx.core;

    exports com.flipkart.varadhi.spi.services;
    exports com.flipkart.varadhi.spi.db;
    exports com.flipkart.varadhi.spi.authn;
    exports com.flipkart.varadhi.spi.db.IamPolicy;
}

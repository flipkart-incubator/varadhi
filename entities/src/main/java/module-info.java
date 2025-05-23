module com.flipkart.varadhi.entities {
    requires static lombok;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.databind;
    requires com.google.common;
    requires jakarta.validation;
    requires jakarta.annotation;

    exports com.flipkart.varadhi.entities;
    exports com.flipkart.varadhi.entities.cluster;
    exports com.flipkart.varadhi.entities.auth;
    exports com.flipkart.varadhi.entities.utils;
    exports com.flipkart.varadhi.entities.filters;
}

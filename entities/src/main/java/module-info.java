module com.flipkart.varadhi.entities {
    requires static lombok;
    requires transitive com.fasterxml.jackson.annotation;
    requires transitive com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.module.paramnames;
    requires com.google.common;
    requires jakarta.validation;
    requires jakarta.annotation;

    exports com.flipkart.varadhi.entities;
    exports com.flipkart.varadhi.entities.cluster;
    exports com.flipkart.varadhi.entities.auth;
    exports com.flipkart.varadhi.entities.utils;
    exports com.flipkart.varadhi.entities.filters;
}

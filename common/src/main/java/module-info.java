module com.flipkart.varadhi.common {

    requires static lombok;
    requires org.slf4j;
    requires com.google.common;
    requires micrometer.core;
    requires com.flipkart.varadhi.entities;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires com.fasterxml.jackson.module.paramnames;
    requires org.apache.commons.collections4;
    requires com.github.benmanes.caffeine;

    exports com.flipkart.varadhi.common.exceptions;
    exports com.flipkart.varadhi.common.reflect;
    exports com.flipkart.varadhi.common;
    exports com.flipkart.varadhi.common.events;
}

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

    exports com.flipkart.varadhi;
    exports com.flipkart.varadhi.exceptions;
    exports com.flipkart.varadhi.reflect;
}

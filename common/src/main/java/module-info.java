module com.flipkart.varadhi.common {

    requires static lombok;
    requires org.slf4j;
    requires micrometer.core;
    requires com.flipkart.varadhi.entities;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires org.apache.commons.collections4;
    requires io.vertx.core;
    requires com.google.common;
    requires org.hibernate.validator;

    exports com.flipkart.varadhi.common;
    exports com.flipkart.varadhi.common.exceptions;
    exports com.flipkart.varadhi.common.reflect;
    exports com.flipkart.varadhi.common.utils;
}

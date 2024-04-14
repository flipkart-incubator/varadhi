module com.flipkart.varadhi.common {
    requires static lombok;
    requires org.slf4j;
    requires com.google.common;
    requires micrometer.core;
    requires com.flipkart.varadhi.entities;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires com.fasterxml.jackson.module.paramnames;

    exports com.flipkart.varadhi;
}

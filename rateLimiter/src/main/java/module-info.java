module com.flipkart.varadhi.rateLimiter {
    requires static lombok;
    requires org.slf4j;
//    requires com.google.common;
    requires org.apache.commons.lang3;
    requires jakarta.validation;
    requires com.fasterxml.jackson.databind;

//    requires com.flipkart.varadhi.common;
//    requires com.flipkart.varadhi.spi;
//    requires com.flipkart.varadhi.entities;

    exports com.flipkart.varadhi.qos.entity;
}

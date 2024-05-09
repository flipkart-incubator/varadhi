module com.flipkart.varadhi.consumer {
    requires static lombok;
    requires org.slf4j;
    requires java.base;
    requires com.google.common;
    requires io.netty.common;

    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.entities;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.core;
    requires jakarta.annotation;
    requires com.fasterxml.jackson.annotation;
}

module com.flipkart.varadhi.consumer {
    requires static lombok;
    requires org.slf4j;
    requires java.base;
    requires com.google.common;
    requires io.netty.common;
    requires org.apache.commons.lang3;

    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.entities;
    requires com.flipkart.varadhi.spi;
    requires jakarta.annotation;
    requires com.fasterxml.jackson.annotation;
}

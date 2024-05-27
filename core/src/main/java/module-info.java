module com.flipkart.varadhi.core {
    requires static lombok;
    requires org.slf4j;
    requires com.fasterxml.jackson.annotation;

    requires com.flipkart.varadhi.common;
    requires com.flipkart.varadhi.spi;
    requires com.flipkart.varadhi.entities;

    exports com.flipkart.varadhi.core.cluster;
}

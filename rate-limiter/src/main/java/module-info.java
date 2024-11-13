module com.flipkart.varadhi.ratelimiter {
    requires static lombok;
    requires org.slf4j;
    requires micrometer.core;
    exports com.flipkart.varadhi.qos.entity;
}

module com.flipkart.varadhi.entities {
    requires static lombok;
    requires com.fasterxml.jackson.annotation;
    requires com.google.common;
    requires jakarta.validation;

    exports com.flipkart.varadhi.entities;
    exports com.flipkart.varadhi.entities.auth;
}

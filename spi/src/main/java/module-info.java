module com.flipkart.varadhi.spi {
    requires static lombok;
    requires com.fasterxml.jackson.databind;
    requires com.flipkart.varadhi.entities;
    requires jakarta.validation;

    exports com.flipkart.varadhi.spi.db;
    exports com.flipkart.varadhi.spi.db.IamPolicy;
    exports com.flipkart.varadhi.spi.services;
    exports com.flipkart.varadhi.spi;
}

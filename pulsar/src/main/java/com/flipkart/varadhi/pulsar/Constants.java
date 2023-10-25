package com.flipkart.varadhi.pulsar;

public class Constants {
    public static final String PROPERTY_MULTI_VALUE_SEPARATOR = ",";

    public static class Producer {
        public static final int MIN_PENDING_MESSAGES = 100;
        public static final int MAX_PENDING_MESSAGES = 1000;
        public static final int MIN_BATCH_SIZE = 10;
        public static final int MAX_BATCH_SIZE = 100;
    }
}

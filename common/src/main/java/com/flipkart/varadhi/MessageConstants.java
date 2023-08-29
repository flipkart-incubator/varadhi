package com.flipkart.varadhi;

import java.util.Set;

public class MessageConstants {

    public static String PRODUCE_CHANNEL_HTTP = "http";
    public static String ANONYMOUS_PRODUCE_IDENTITY = "Anonymous";

    public static class Headers {
        public static String VARADHI_HEADER_PREFIX = "x_";
        public static String PRODUCE_TIMESTAMP = "x_restbus_produce_timestamp";
        public static String PRODUCE_REGION = "x_restbus_produce_region";
        public static String PRODUCE_IDENTITY = "x_restbus_produce_identity";
        public static String FORWARDED_FOR = "x-forwarded-for";
        public static String MESSAGE_ID = "x_restbus_message_id";
        public static String GROUP_ID = "x_restbus_group_id";

        public static Set<String> REQUIRED_HEADERS =
                Set.of(MESSAGE_ID, PRODUCE_IDENTITY, PRODUCE_REGION, PRODUCE_TIMESTAMP);
    }
}

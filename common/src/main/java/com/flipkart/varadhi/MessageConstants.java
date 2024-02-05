package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.StandardHeaders;

import java.util.List;

public class MessageConstants {
    public static String API_PROTOCOL_HTTP = "http";
    public static String ANONYMOUS_PRODUCE_IDENTITY = "Anonymous";

    public static class Headers {
        public static List<String> REQUIRED_HEADERS =
                List.of(
                        StandardHeaders.MESSAGE_ID,
                        StandardHeaders.PRODUCE_IDENTITY,
                        StandardHeaders.PRODUCE_REGION,
                        StandardHeaders.PRODUCE_TIMESTAMP
                );
    }
}

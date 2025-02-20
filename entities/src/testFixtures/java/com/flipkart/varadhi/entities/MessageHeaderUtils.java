package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;

import java.util.List;
import java.util.Map;

public class MessageHeaderUtils {

    public static MessageHeaderConfiguration fetchDummyHeaderConfiguration() {
        return new MessageHeaderConfiguration(
            Map.ofEntries(
                Map.entry(StandardHeaders.MSG_ID, "X_MESSAGE_ID"),
                Map.entry(StandardHeaders.GROUP_ID, "X_GROUP_ID"),
                Map.entry(StandardHeaders.CALLBACK_CODE, "X_CALLBACK_CODES"),
                Map.entry(StandardHeaders.REQUEST_TIMEOUT, "X_REQUEST_TIMEOUT"),
                Map.entry(StandardHeaders.REPLY_TO_HTTP_URI, "X_REPLY_TO_HTTP_URI"),
                Map.entry(StandardHeaders.REPLY_TO_HTTP_METHOD, "X_REPLY_TO_HTTP_METHOD"),
                Map.entry(StandardHeaders.REPLY_TO, "X_REPLY_TO"),
                Map.entry(StandardHeaders.HTTP_URI, "X_HTTP_URI"),
                Map.entry(StandardHeaders.HTTP_METHOD, "X_HTTP_METHOD"),
                Map.entry(StandardHeaders.CONTENT_TYPE, "X_CONTENT_TYPE"),
                Map.entry(StandardHeaders.PRODUCE_IDENTITY, "X_PRODUCE_IDENTITY"),
                Map.entry(StandardHeaders.PRODUCE_REGION, "X_PRODUCE_REGION"),
                Map.entry(StandardHeaders.PRODUCE_TIMESTAMP, "X_PRODUCE_TIMESTAMP")
            ),
            List.of("X_", "x_"),
            100,
            (5 * 1024 * 1024)
        );
    }
}

package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.config.MessageConfiguration;
import com.flipkart.varadhi.entities.constants.MessageHeaders;

import java.util.List;
import java.util.Map;

public class MessageHeaderUtils {

    public static MessageConfiguration fetchConfiguration(boolean filterNonCompliantHeaders) {
        return new MessageConfiguration(
            Map.ofEntries(
                Map.entry(MessageHeaders.MSG_ID, "X_MESSAGE_ID"),
                Map.entry(MessageHeaders.GROUP_ID, "X_GROUP_ID"),
                Map.entry(MessageHeaders.CALLBACK_CODE, "X_CALLBACK_CODES"),
                Map.entry(MessageHeaders.REQUEST_TIMEOUT, "X_REQUEST_TIMEOUT"),
                Map.entry(MessageHeaders.REPLY_TO_HTTP_URI, "X_REPLY_TO_HTTP_URI"),
                Map.entry(MessageHeaders.REPLY_TO_HTTP_METHOD, "X_REPLY_TO_HTTP_METHOD"),
                Map.entry(MessageHeaders.REPLY_TO, "X_REPLY_TO"),
                Map.entry(MessageHeaders.HTTP_URI, "X_HTTP_URI"),
                Map.entry(MessageHeaders.HTTP_METHOD, "X_HTTP_METHOD"),
                Map.entry(MessageHeaders.CONTENT_TYPE, "X_CONTENT_TYPE"),
                Map.entry(MessageHeaders.PRODUCE_IDENTITY, "X_PRODUCE_IDENTITY"),
                Map.entry(MessageHeaders.PRODUCE_REGION, "X_PRODUCE_REGION"),
                Map.entry(MessageHeaders.PRODUCE_TIMESTAMP, "X_PRODUCE_TIMESTAMP")
            ),
            List.of("X_", "x_"),
            100,
            (5 * 1024 * 1024),
            filterNonCompliantHeaders
        );
    }

    public static MessageConfiguration fetchDummyHeaderConfigurationWithParams(boolean filterNonCompliantHeaders) {
        return fetchConfiguration(filterNonCompliantHeaders);
    }

    public static MessageConfiguration fetchDummyHeaderConfiguration() {
        return fetchConfiguration(true);
    }

}

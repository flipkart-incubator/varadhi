package com.flipkart.varadhi.entities;

import java.util.List;

public class TestStdHeaders {

    public static StdHeaders get() {
        return new StdHeaders(
            List.of("X_", "x_"),
            "X_MESSAGE_ID",
            "X_GROUP_ID",
            "X_CALLBACK_CODES",
            "X_REQUEST_TIMEOUT",
            "X_REPLY_TO_HTTP_URI",
            "X_REPLY_TO_HTTP_METHOD",
            "X_REPLY_TO",
            "X_HTTP_URI",
            "X_HTTP_METHOD",
            "X_CONTENT_TYPE",
            "X_PRODUCE_IDENTITY",
            "X_PRODUCE_REGION",
            "X_PRODUCE_TIMESTAMP"
        );
    }
}

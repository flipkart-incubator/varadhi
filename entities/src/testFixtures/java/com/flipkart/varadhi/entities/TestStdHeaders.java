package com.flipkart.varadhi.entities;

import java.util.List;

public class TestStdHeaders {

    public static StdHeaders get() {
        return new StdHeaders(
            List.of("X_", "X-"),
            new HeaderSpec("X_MESSAGE_ID", RequiredBy.produce()),
            new HeaderSpec("X_GROUP_ID", RequiredBy.None),
            new HeaderSpec("X_CALLBACK_CODES", RequiredBy.None),
            new HeaderSpec("X_REQUEST_TIMEOUT", RequiredBy.None),
            new HeaderSpec("X_REPLY_TO_HTTP_URI", RequiredBy.None),
            new HeaderSpec("X_REPLY_TO_HTTP_METHOD", RequiredBy.None),
            new HeaderSpec("X_REPLY_TO", RequiredBy.None),
            new HeaderSpec("X_HTTP_URI", RequiredBy.Queue),
            new HeaderSpec("X_HTTP_METHOD", RequiredBy.Queue),
            new HeaderSpec("X_CONTENT_TYPE", RequiredBy.None),
            new HeaderSpec("X_PRODUCE_IDENTITY", RequiredBy.None),
            new HeaderSpec("X_PRODUCE_REGION", RequiredBy.None),
            new HeaderSpec("X_PRODUCE_TIMESTAMP", RequiredBy.None)
        );
    }
}

package com.flipkart.varadhi.entities;

import java.util.List;

public class TestStdHeaders {

    public static StdHeaders get() {
        return new StdHeaders(
            List.of("X_", "X-"),
            new HeaderSpec("X_MESSAGE_ID", RequiredBy.mandatoryHeaderRequiredForProduce()),
            "X_GROUP_ID",
            "X_CALLBACK_CODES",
            "X_REQUEST_TIMEOUT",
            new HeaderSpec("X_REPLY_TO_HTTP_URI", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO_HTTP_METHOD", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_HTTP_URI", RequiredBy.Queue),
            new HeaderSpec("X_HTTP_METHOD", RequiredBy.Queue),
            "X_CONTENT_TYPE",
            "X_PRODUCE_IDENTITY",
            "X_PRODUCE_REGION",
            "X_PRODUCE_TIMESTAMP"
        );
    }
}

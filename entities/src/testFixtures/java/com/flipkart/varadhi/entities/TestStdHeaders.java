package com.flipkart.varadhi.entities;

import java.util.List;

public class TestStdHeaders {

    public static StdHeaders get() {
        return new StdHeaders(
            List.of("X_", "X-"),
            new HeaderSpec("X_MESSAGE_ID", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_GROUP_ID", null),
            new HeaderSpec("X_CALLBACK_CODES", null),
            new HeaderSpec("X_REQUEST_TIMEOUT", null),
            new HeaderSpec("X_REPLY_TO_HTTP_URI", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO_HTTP_METHOD", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO", RequiredBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_HTTP_URI", RequiredBy.Queue),
            new HeaderSpec("X_HTTP_METHOD", RequiredBy.Queue),
            new HeaderSpec("X_CONTENT_TYPE", RequiredBy.Both),
            new HeaderSpec("X_PRODUCE_IDENTITY", RequiredBy.Both),
            new HeaderSpec("X_PRODUCE_REGION", RequiredBy.Both),
            new HeaderSpec("X_PRODUCE_TIMESTAMP", RequiredBy.Both)
        );
    }
}

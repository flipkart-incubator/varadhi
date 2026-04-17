package com.flipkart.varadhi.entities;

import java.util.List;

public class TestStdHeaders {

    public static StdHeaders get() {
        return new StdHeaders(
            List.of("X_", "X-"),
            new HeaderSpec("X_MESSAGE_ID", MandatoryBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_GROUP_ID", null),
            new HeaderSpec("X_CALLBACK_CODES", null),
            new HeaderSpec("X_REQUEST_TIMEOUT", null),
            new HeaderSpec("X_REPLY_TO_HTTP_URI", MandatoryBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO_HTTP_METHOD", MandatoryBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_REPLY_TO", MandatoryBy.mandatoryHeaderRequiredForProduce()),
            new HeaderSpec("X_HTTP_URI", MandatoryBy.Queue),
            new HeaderSpec("X_HTTP_METHOD", MandatoryBy.Queue),
            new HeaderSpec("X_CONTENT_TYPE", MandatoryBy.Both),
            new HeaderSpec("X_PRODUCE_IDENTITY", MandatoryBy.Both),
            new HeaderSpec("X_PRODUCE_REGION", MandatoryBy.Both),
            new HeaderSpec("X_PRODUCE_TIMESTAMP", MandatoryBy.Both)
        );
    }
}

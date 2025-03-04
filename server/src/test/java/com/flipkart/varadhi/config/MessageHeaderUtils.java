package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.TestStdHeaders;

public class MessageHeaderUtils {

    public static MessageConfiguration fetchTestConfiguration(boolean filterNonCompliantHeaders) {
        return new MessageConfiguration(TestStdHeaders.get(), 100, (5 * 1024 * 1024), filterNonCompliantHeaders);
    }

    public static MessageConfiguration fetchTestConfiguration() {
        return fetchTestConfiguration(true);
    }
}

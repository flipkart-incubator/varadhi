package com.flipkart.varadhi.core.config;

import com.flipkart.varadhi.entities.TestStdHeaders;

public class MessageHeaderUtils {

    public static MessageConfiguration getTestConfiguration(boolean filterNonCompliantHeaders) {
        return new MessageConfiguration(TestStdHeaders.get(), 100, (5 * 1024 * 1024), filterNonCompliantHeaders);
    }

    public static MessageConfiguration getTestConfiguration() {
        return getTestConfiguration(true);
    }
}

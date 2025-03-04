package com.flipkart.varadhi.common.utils;

import io.micrometer.core.instrument.Tags;
import io.vertx.core.MultiMap;

public class MetricsUtil {

    public static Tags getCustomHttpHeaders(MultiMap multiMap) {
        Tags tags = Tags.empty();
        return tags;
    }

    public static Boolean isSuccessfulResponse(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    public static String categorizeStatusCode(int statusCode) {
        if (isSuccessfulResponse(statusCode)) {
            return "2XX";
        } else if (statusCode >= 400 && statusCode < 500) {
            return "4XX";
        } else if (statusCode >= 500) {
            return "5XX";
        }
        return String.valueOf(statusCode);
    }
}

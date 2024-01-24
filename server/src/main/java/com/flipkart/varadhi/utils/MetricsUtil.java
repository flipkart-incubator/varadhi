package com.flipkart.varadhi.utils;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.vertx.core.MultiMap;

import java.util.Arrays;
import java.util.List;

import static com.flipkart.varadhi.Constants.Tags.*;

public class MetricsUtil {

    public static Tags getCustomHttpHeaders(MultiMap multiMap) {

        Tags tags = Tags.empty();
        List<String> tagsToConsider = Arrays.asList(TAG_NAME_TOPIC, TAG_NAME_REGION,
                TAG_NAME_PROJECT, TAG_NAME_IDENTITY, TAG_NAME_HOST);

        for (String tagName : tagsToConsider) {
            if (multiMap.contains(tagName)) {
                String tagValue = multiMap.get(tagName);
                tags = tags.and(Tag.of(tagName, tagValue));
            }
        }
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

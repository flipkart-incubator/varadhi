package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.exceptions.ArgumentException;
import com.google.common.collect.Multimap;

import static com.flipkart.varadhi.MessageConstants.Headers.REQUIRED_HEADERS;

public class MessageHelper {

    public static void ensureRequiredHeaders(Multimap<String, String> requestHeaders) {
        REQUIRED_HEADERS.forEach(key -> {
            if (!requestHeaders.containsKey(key)) {
                throw new ArgumentException(String.format("Missing required header %s", key));
            }
        });
    }
}

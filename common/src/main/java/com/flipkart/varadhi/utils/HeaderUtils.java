package com.flipkart.varadhi.utils;

import java.util.Map;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.MessageConstants.HEADER_PREFIX;

public class HeaderUtils {
    public static Map<String, String> getVaradhiHeader(Map<String, String> headers) {
        return headers
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().toLowerCase().startsWith(HEADER_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }
}

package com.flipkart.varadhi.entities.constants;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

public class HeaderUtils {
    //Assuming it's initialized at the time of bootstrapping
    public static List<String> allowedPrefix;
    public static Map<StandardHeaders, String> mapping;
    public static Integer headerValueSizeMax;
    public static Integer maxRequestSize;

    // Static method to initialize fields via constructor-like behavior
    public static void initialize(MessageHeaderConfiguration config) {
        HeaderUtils.allowedPrefix = config.getAllowedPrefix();
        HeaderUtils.mapping = config.getMapping();
        HeaderUtils.headerValueSizeMax = config.getHeaderValueSizeMax();
        HeaderUtils.maxRequestSize = config.getMaxRequestSize();
    }

    // Method to check if all fields are initialized
    public static void checkInitialization() {

    }

    public static List<String> getRequiredHeaders(MessageHeaderConfiguration messageHeaderConfiguration) {
        return List.of(
                messageHeaderConfiguration.getMapping().get(StandardHeaders.MSG_ID)
        );
    }

    public static void ensureRequiredHeaders(
            MessageHeaderConfiguration messageHeaderConfiguration, Multimap<String, String> headers
    ) {
        getRequiredHeaders(messageHeaderConfiguration).forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public static Multimap<String, String> copyVaradhiHeaders(
            Multimap<String, String> headers, List<String> allowedPrefix
    ) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            String key = entry.getKey();
            boolean validPrefix = allowedPrefix.stream().anyMatch(key::startsWith);
            if (validPrefix) {
                varadhiHeaders.put(key, entry.getValue());
            }
        });
        return varadhiHeaders;
    }

    public static MessageHeaderConfiguration fetchDummyHeaderConfiguration() {
        return new MessageHeaderConfiguration(
                Map.ofEntries(
                        Map.entry(StandardHeaders.MSG_ID, "X_MESSAGE_ID"),
                        Map.entry(StandardHeaders.GROUP_ID, "X_GROUP_ID"),
                        Map.entry(StandardHeaders.CALLBACK_CODE, "X_CALLBACK_CODES"),
                        Map.entry(StandardHeaders.REQUEST_TIMEOUT, "X_REQUEST_TIMEOUT"),
                        Map.entry(StandardHeaders.REPLY_TO_HTTP_URI, "X_REPLY_TO_HTTP_URI"),
                        Map.entry(StandardHeaders.REPLY_TO_HTTP_METHOD, "X_REPLY_TO_HTTP_METHOD"),
                        Map.entry(StandardHeaders.REPLY_TO, "X_REPLY_TO"),
                        Map.entry(StandardHeaders.HTTP_URI, "X_HTTP_URI"),
                        Map.entry(StandardHeaders.HTTP_METHOD, "X_HTTP_METHOD"),
                        Map.entry(StandardHeaders.CONTENT_TYPE, "X_CONTENT_TYPE"),
                        Map.entry(StandardHeaders.PRODUCE_IDENTITY, "X_PRODUCE_IDENTITY"),
                        Map.entry(StandardHeaders.PRODUCE_REGION, "X_PRODUCE_REGION"),
                        Map.entry(StandardHeaders.PRODUCE_TIMESTAMP, "X_PRODUCE_TIMESTAMP")
                ),
                List.of("X_", "x_"),
                100,
                (5 * 1024 * 1024)
        );
    }
}

package com.flipkart.varadhi.entities.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

public class HeaderUtils {
    private static List<String> allowedPrefix;
    private static Map<StandardHeaders, String> mapping;
    public static Integer headerValueSizeMax;
    public static Integer maxRequestSize;
    private static Boolean filterNonCompliantHeaders;
    private static Boolean initialized;

    public static synchronized void initialize(MessageHeaderConfiguration config) {
        if (initialized != null && initialized) {
            return; // Prevent re-initialization if already initialized
        }
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }
        HeaderUtils.allowedPrefix = config.allowedPrefix();
        HeaderUtils.mapping = ImmutableMap.copyOf(config.mapping());
        HeaderUtils.headerValueSizeMax = config.headerValueSizeMax();
        HeaderUtils.maxRequestSize = config.maxRequestSize();
        HeaderUtils.filterNonCompliantHeaders = config.filterNonCompliantHeaders();
        HeaderUtils.initialized = true;
    }

    public static synchronized void deInitialize() {
        HeaderUtils.initialized = false;
    }

    public static String getHeader(StandardHeaders header) {
        checkInitialization();
        return mapping.get(header);
    }

    public static void checkInitialization() {
        if (HeaderUtils.initialized != Boolean.TRUE) {
            throw new IllegalStateException("One or more required static fields are not initialized");
        }
    }

    public static List<String> getRequiredHeaders() {
        return List.of(HeaderUtils.getHeader(StandardHeaders.MSG_ID));
    }

    public static void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public static Multimap<String, String> returnVaradhiRecognizedHeaders(Multimap<String, String> headers) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        boolean shouldFilter = HeaderUtils.filterNonCompliantHeaders.equals(Boolean.TRUE);

        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            if (shouldFilter) {
                boolean validPrefix = HeaderUtils.allowedPrefix.stream().anyMatch(key::startsWith);
                if (validPrefix) {
                    varadhiHeaders.put(key.toUpperCase(), entry.getValue());
                }
            } else {
                varadhiHeaders.put(key.toUpperCase(), entry.getValue());
            }
        }
        return varadhiHeaders;
    }

}

package com.flipkart.varadhi.entities.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

public class HeaderUtils {
    public static List<String> allowedPrefix;
    private static Map<StandardHeaders, String> mapping;
    public static Integer headerValueSizeMax;
    public static Integer maxRequestSize;
    private static Boolean initialized;
    public static void initialize(MessageHeaderConfiguration config) {
        if (initialized != null && initialized) {
            return; // Prevent re-initialization if already initialized
        }
        HeaderUtils.allowedPrefix = config.allowedPrefix();
        //set immutable map
        HeaderUtils.mapping = ImmutableMap.copyOf(config.mapping());
        HeaderUtils.headerValueSizeMax = config.headerValueSizeMax();
        HeaderUtils.maxRequestSize = config.maxRequestSize();
        HeaderUtils.initialized = true;
    }

    public static String getHeader(StandardHeaders header){
        checkInitialization();
        return mapping.get(header);
    }
    public static void checkInitialization() {
        if (HeaderUtils.initialized != Boolean.TRUE) {
            throw new IllegalStateException("One or more required static fields are not initialized");
        }
    }

    public static List<String> getRequiredHeaders() {
        return List.of(
                HeaderUtils.getHeader(StandardHeaders.MSG_ID)
        );
    }

    public static void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public static Multimap<String, String> copyVaradhiHeaders(
            Multimap<String, String> headers
    ) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            String key = entry.getKey();
            boolean validPrefix = HeaderUtils.allowedPrefix.stream().anyMatch(key::startsWith);
            if (validPrefix) {
                varadhiHeaders.put(key, entry.getValue());
            }
        });
        return varadhiHeaders;
    }
}

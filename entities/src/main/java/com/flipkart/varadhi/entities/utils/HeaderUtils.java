package com.flipkart.varadhi.entities.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import io.vertx.core.MultiMap;

public class HeaderUtils {
    private static List<String> allowedPrefix;
    private static Map<MessageHeaders, String> mapping;
    public static int headerValueSizeMax;
    public static int maxRequestSize;
    private static boolean filterNonCompliantHeaders;
    private static boolean initialized;

    public static synchronized void initialize(MessageHeaderConfiguration config) {
        if (initialized) {
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

    public static String getHeader(MessageHeaders header) {
        checkInitialization();
        return mapping.get(header);
    }

    public static void checkInitialization() {
        if (!HeaderUtils.initialized) {
            throw new IllegalStateException("Header configuration not yet initialized.");
        }
    }

    public static List<String> getRequiredHeaders() {
        return List.of(HeaderUtils.getHeader(MessageHeaders.MSG_ID));
    }

    public static void ensureRequiredHeaders(MultiMap headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.contains(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public static Multimap<String, String> returnVaradhiRecognizedHeaders(MultiMap headers) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            if (HeaderUtils.filterNonCompliantHeaders) {
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

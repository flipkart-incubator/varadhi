package com.flipkart.varadhi.entities.utils;

import com.flipkart.varadhi.entities.config.MessageConfiguration;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import lombok.Getter;

public class HeaderUtils {
    private static final HeaderUtils DEFAULT_INSTANCE = new HeaderUtils();
    @Getter
    private boolean initialized = false;
    public MessageConfiguration messageConfiguration;

    private HeaderUtils() {
    }

    @VisibleForTesting
    protected HeaderUtils(MessageConfiguration configuration) {
        messageConfiguration = configuration;
    }

    private void initialize(MessageConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }
        messageConfiguration = config;
        initialized = true;
    }

    public static synchronized void init(MessageConfiguration messageConfiguration) {
        // Prevent re-initialization if already initialized
        if (DEFAULT_INSTANCE.isInitialized()) {
            throw new IllegalStateException("Already initialized");
        }
        DEFAULT_INSTANCE.initialize(messageConfiguration);
    }

    public static HeaderUtils getInstance() {
        return DEFAULT_INSTANCE;
    }

    public static MessageConfiguration getMessageConfig() {
        return DEFAULT_INSTANCE.messageConfiguration;
    }

    public static String getMsgIdHeaderKey() {
        return getMessageConfig().getMsgIdHeaderKey();
    }

    public static String getHeader(MessageHeaders header) {
        return DEFAULT_INSTANCE.messageConfiguration.headerMapping().get(header);
    }

    public static List<String> getRequiredHeaders() {
        return List.of(DEFAULT_INSTANCE.messageConfiguration.headerMapping().get(MessageHeaders.MSG_ID));
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
        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            if (DEFAULT_INSTANCE.messageConfiguration.filterNonCompliantHeaders()) {
                boolean validPrefix = DEFAULT_INSTANCE.messageConfiguration.allowedPrefix()
                                                                           .stream()
                                                                           .anyMatch(key::startsWith);
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

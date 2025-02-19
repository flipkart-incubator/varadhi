package com.flipkart.varadhi.entities.constants;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;

public class StandardHeaders {
    //Assuming it's initialized at the time of bootstrapping
    public static List<String> allowedPrefix;
    public static String callbackCodes;
    public static String requestTimeout;
    public static String replyToHttpUriHeader;
    public static String replyToHttpMethodHeader;
    public static String replyToHeader;
    public static String httpUriHeader;
    public static String httpMethodHeader;
    public static String httpContentType;
    public static String groupIdHeader;

    public static String msgIdHeader;
    public static Integer headerValueSizeMax;
    public static String produceTimestamp;
    public static String produceRegion;
    public static String produceIdentity;
    public static Integer maxRequestSize;

    // Static method to initialize fields via constructor-like behavior
    public static void initialize(MessageHeaderConfiguration config) {
        StandardHeaders.allowedPrefix = config.getAllowedPrefix();
        StandardHeaders.callbackCodes = config.getCallbackCodes();
        StandardHeaders.requestTimeout = config.getRequestTimeout();
        StandardHeaders.replyToHttpUriHeader = config.getReplyToHttpUriHeader();
        StandardHeaders.replyToHttpMethodHeader = config.getReplyToHttpMethodHeader();
        StandardHeaders.replyToHeader = config.getReplyToHeader();
        StandardHeaders.httpUriHeader = config.getHttpUriHeader();
        StandardHeaders.httpMethodHeader = config.getHttpMethodHeader();
        StandardHeaders.httpContentType = config.getHttpContentType();
        StandardHeaders.groupIdHeader = config.getGroupIdHeader();
        StandardHeaders.msgIdHeader = config.getMsgIdHeader();
        StandardHeaders.headerValueSizeMax = config.getHeaderValueSizeMax();
        StandardHeaders.produceTimestamp = config.getProduceTimestamp();
        StandardHeaders.produceRegion = config.getProduceRegion();
        StandardHeaders.produceIdentity = config.getProduceIdentity();
        StandardHeaders.maxRequestSize = config.getMaxRequestSize();
    }

    // Method to check if all fields are initialized
    public static void checkInitialization() {
        if (allowedPrefix == null || callbackCodes == null || requestTimeout == null ||
                replyToHttpUriHeader == null || replyToHttpMethodHeader == null || replyToHeader == null ||
                httpUriHeader == null || httpMethodHeader == null || httpContentType == null ||
                groupIdHeader == null || msgIdHeader == null || headerValueSizeMax == null ||
                produceTimestamp == null || produceRegion == null || produceIdentity == null ||
                maxRequestSize == null) {
            throw new IllegalStateException("Standard Headers not properly initialized");
        }
    }

    public static List<String> getRequiredHeaders(MessageHeaderConfiguration messageHeaderConfiguration) {
        return List.of(
                messageHeaderConfiguration.getMsgIdHeader()
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
                List.of("X_", "x_"),
                "X_CALLBACK_CODES",
                "X_REQUEST_TIMEOUT",
                "X_REPLY_TO_HTTP_URI",
                "X_REPLY_TO_HTTP_METHOD",
                "X_REPLY_TO",
                "X_HTTP_URI",
                "X_HTTP_METHOD",
                "X_CONTENT_TYPE",
                "X_GROUP_ID",
                "X_MESSAGE_ID",
                100,
                "X_PRODUCE_TIMESTAMP",
                "X_PRODUCE_REGION",
                "X_PRODUCE_IDENTITY",
                (5 * 1024 * 1024)
        );
    }
}

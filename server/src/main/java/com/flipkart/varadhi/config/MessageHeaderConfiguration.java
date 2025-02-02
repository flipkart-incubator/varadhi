package com.flipkart.varadhi.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageHeaderConfiguration {
    private List<String> msgHeaderPrefix;

    // Callback codes header
    private String callbackCodes;

    // Consumer timeout header (indefinite message consumer timeout)
    private String requestTimeout;

    // HTTP related headers
    private String replyToHttpUriHeader;
    private String replyToHttpMethodHeader;
    private String replyToHeader;
    private String httpUriHeader;
    private String httpMethodHeader;
    private String httpContentType;

    // Group ID & Msg ID header used to correlate messages
    private String groupIdHeader;
    private String msgIdHeader;


    //removal candidates

    // Used in group flow
    private String exchangeNameHeader;
    // Original topic name (maintains original queue name during changes)
    private String originalTopicName;

    public static MessageHeaderConfiguration buildFromConfig(MessageHeaderConfiguration providedConfig) {
        if (providedConfig == null) {
            throw new IllegalArgumentException("Provided configuration cannot be null");
        }

        for (Method method : providedConfig.getClass().getMethods()) {
            if (method.getName().startsWith("get") && method.getParameterCount() == 0) {
                try {
                    Object value = method.invoke(providedConfig);
                    if (value == null) {
                        throw new IllegalArgumentException(method.getName() + " cannot be null");
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error invoking getter method: " + method.getName(), e);
                }
            }
        }

        return providedConfig;
    }

    public static boolean validateHeaderMapping(MessageHeaderConfiguration messageHeaderConfiguration)
            throws IllegalAccessException {
        if (messageHeaderConfiguration == null || messageHeaderConfiguration.getMsgHeaderPrefix() == null
                || messageHeaderConfiguration.getMsgHeaderPrefix().isEmpty()) {
            throw new IllegalArgumentException("Message header configuration or prefix list cannot be null or empty");
        }
        for (Field field : MessageHeaderConfiguration.class.getDeclaredFields()) {
            if (field.getName().equals("msgHeaderPrefix")) {
                continue;
            }
            Object value = field.get(messageHeaderConfiguration);
            if (value == null) {
                throw new IllegalArgumentException("Header value cannot be null: " + field.getName());
            }
            if (!startsWithValidPrefix(messageHeaderConfiguration.getMsgHeaderPrefix(), (String) value)) {
                throw new IllegalArgumentException(
                        String.format("Header '%s' with value '%s' doesn't start with any valid prefix",
                                field.getName(), value
                        ));
            }
        }
        return true;
    }

    private static boolean startsWithValidPrefix(List<String> prefixList, String value) {
        if (prefixList == null || value == null) {
            return false;
        }
        for (String prefix : prefixList) {
            if (prefix == null) {
                continue;
            }
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}

package com.flipkart.varadhi.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class MessageHeaderConfiguration {
    private List<String> msgHeaderPrefix;
    private String messageId;
    private String groupId;
    private String httpUri;
    private String httpMethod;
    private String replyTo;
    private String replyToHttpUri;
    private String replyToHttpMethod;
    private String responseCode;
    private String responseBody;
    private String contentType;

    public static MessageHeaderConfiguration getDefaultMessageHeaders() {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("X_", "X-"))
                .messageId("X_RESTBUS_MESSAGE_ID")
                .groupId("X_RESTBUS_GROUP_ID")
                .httpUri("X_RESTBUS_HTTP_URI")
                .httpMethod("X_RESTBUS_HTTP_METHOD")
                .replyTo("X_RESTBUS_REPLY_TO")
                .replyToHttpUri("X_RESTBUS_REPLY_TO_HTTP_URI")
                .replyToHttpMethod("X_RESTBUS_REPLY_TO_HTTP_METHOD")
                .responseCode("X-RESTBUS_RESPONSE_CODE")
                .responseBody("X_RESTBUS_RESPONSE_BODY")
                .contentType("X_RESTBUS_CONTENT_TYPE")
                .build();
    }

    public static boolean validateHeaderMapping(MessageHeaderConfiguration messageHeaderConfiguration)
            throws IllegalAccessException {
            for (Field field : MessageHeaderConfiguration.class.getDeclaredFields()) {
                if (field.getName().equals("msgHeaderPrefix")) continue;
                Object value = field.get(messageHeaderConfiguration);
                if (!startsWithValidPrefix(messageHeaderConfiguration.getMsgHeaderPrefix(), (String) value)) {
                    return false;
                }
                return true;
            }
        return false;
    }

    private static boolean startsWithValidPrefix(List<String> prefixList, String value) {
        for (String prefix : prefixList) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}

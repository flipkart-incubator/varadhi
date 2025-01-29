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
                .msgHeaderPrefix(List.of("VARADHI"))
                .messageId("VARADHI_MESSAGE_ID")
                .groupId("VARADHI_GROUP_ID")
                .httpUri("VARADHI_HTTP_URI")
                .httpMethod("VARADHI_HTTP_METHOD")
                .replyTo("VARADHI_REPLY_TO")
                .replyToHttpUri("VARADHI_REPLY_TO_HTTP_URI")
                .replyToHttpMethod("VARADHI_REPLY_TO_HTTP_METHOD")
                .responseCode("VARADHI-RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .contentType("VARADHI_CONTENT_TYPE")
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

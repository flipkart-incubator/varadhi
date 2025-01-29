package com.flipkart.varadhi.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageHeaderConfiguration {
    private List<String> msgHeaderPrefix;
    // NFR strategy & handler header
    private String nfrMessageOrderPathHeader;
    private String nfrMessageHeader1;
    private String nfrMessageHeader2;

    // Auth Proxy Filter header
    private String proxyUserHeader;

    // Forwarded IPs header
    private String xForwardedFor;

    // Whitelisting of clientIds header
    private String customTargetClientIdHeader;

    // Callback codes header
    private String callbackCodes;

    // Used in group flow
    private String exchangeNameHeader;

    // Original topic name (maintains original queue name during changes)
    private String originalTopicName;

    // Consumer timeout header (indefinite message consumer timeout)
    private String requestTimeout;

    // Authenticated user header (mirror topic handler, not ideal processing)
    private String authnUser;

    // HTTP related headers
    private String replyToHttpUriHeader;
    private String replyToHttpMethodHeader;
    private String replyToHeader;
    private String httpUriHeader;
    private String httpMethodHeader;
    private String httpContentType;

    // Bridged message indicator header
    private String bridged;

    // Group ID & Msg ID header used to correlate messages
    private String groupIdHeader;
    private String msgIdHeader;


    // Response-related headers
    private String responseCode;
    private String responseBody;

    public static MessageHeaderConfiguration buildConfigWithDefaults(MessageHeaderConfiguration providedConfig) {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(getOrDefault(providedConfig.getMsgHeaderPrefix(), List.of("VARADHI_", "VARADHI-")))
                .nfrMessageOrderPathHeader(getOrDefault(providedConfig.getNfrMessageOrderPathHeader(), "VARADHI-PERF-ORDERPATH"))
                .nfrMessageHeader1(getOrDefault(providedConfig.getNfrMessageHeader1(), "VARADHI_PERF_TEST"))
                .nfrMessageHeader2(getOrDefault(providedConfig.getNfrMessageHeader2(), "VARADHI-PERF-TEST"))
                .proxyUserHeader(getOrDefault(providedConfig.getProxyUserHeader(), "VARADHI-Proxy-User"))
                .xForwardedFor(getOrDefault(providedConfig.getXForwardedFor(), "VARADHI-FORWARDED-FOR"))
                .customTargetClientIdHeader(getOrDefault(providedConfig.getCustomTargetClientIdHeader(), "VARADHI_TARGET_CLIENT_ID_HEADER"))
                .callbackCodes(getOrDefault(providedConfig.getCallbackCodes(), "VARADHI_CALLBACK_CODES"))
                .exchangeNameHeader(getOrDefault(providedConfig.getExchangeNameHeader(), "VARADHI_EXCHANGE_NAME"))
                .originalTopicName(getOrDefault(providedConfig.getOriginalTopicName(), "VARADHI_ORIGINAL_TOPIC_NAME"))
                .requestTimeout(getOrDefault(providedConfig.getRequestTimeout(), "VARADHI_REQUEST_TIMEOUT"))
                .authnUser(getOrDefault(providedConfig.getAuthnUser(), "VARADHI_AUTHN_USER"))
                .replyToHttpUriHeader(getOrDefault(providedConfig.getReplyToHttpUriHeader(), "VARADHI_REPLY_TO_HTTP_URI"))
                .replyToHttpMethodHeader(getOrDefault(providedConfig.getReplyToHttpMethodHeader(), "VARADHI_REPLY_TO_HTTP_METHOD"))
                .replyToHeader(getOrDefault(providedConfig.getReplyToHeader(), "VARADHI_REPLY_TO"))
                .httpUriHeader(getOrDefault(providedConfig.getHttpUriHeader(), "VARADHI_HTTP_URI"))
                .httpMethodHeader(getOrDefault(providedConfig.getHttpMethodHeader(), "VARADHI_HTTP_METHOD"))
                .httpContentType(getOrDefault(providedConfig.getHttpContentType(), "VARADHI_CONTENT_TYPE"))
                .bridged(getOrDefault(providedConfig.getBridged(), "VARADHI_BRIDGED"))
                .groupIdHeader(getOrDefault(providedConfig.getGroupIdHeader(), "VARADHI_GROUP_ID"))
                .msgIdHeader(getOrDefault(providedConfig.getMsgIdHeader(), "VARADHI_MSG_ID"))
                .responseCode(getOrDefault(providedConfig.getResponseCode(), "VARADHI_RESPONSE_CODE"))
                .responseBody(getOrDefault(providedConfig.getResponseBody(), "VARADHI_RESPONSE_BODY"))
                .build();
    }

    private static <T> T getOrDefault(T providedValue, T defaultValue) {
        return providedValue != null ? providedValue : defaultValue;
    }

    public static boolean validateHeaderMapping(MessageHeaderConfiguration messageHeaderConfiguration)
            throws IllegalAccessException {
        for (Field field : MessageHeaderConfiguration.class.getDeclaredFields()) {
            if (field.getName().equals("msgHeaderPrefix")) {
                continue;
            }
            Object value = field.get(messageHeaderConfiguration);
            if (!startsWithValidPrefix(messageHeaderConfiguration.getMsgHeaderPrefix(), (String) value)) {
                return false;
            }
        }
        return true;
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

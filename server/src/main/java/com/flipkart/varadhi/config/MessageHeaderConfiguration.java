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

    public static MessageHeaderConfiguration getDefaultMessageHeaders() {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(List.of("X_", "X-"))
                .nfrMessageOrderPathHeader("X-PERF-ORDERPATH")
                .nfrMessageHeader1("X_PERF_TEST")
                .nfrMessageHeader2("X-PERF-TEST")
                .proxyUserHeader("X-Proxy-User")
                .xForwardedFor("X-FORWARDED-FOR")
                .customTargetClientIdHeader("X_TARGET_CLIENT_ID_HEADER")
                .callbackCodes("X_CALLBACK_CODES")
                .exchangeNameHeader("X_EXCHANGE_NAME")
                .originalTopicName("X_ORIGINAL_TOPIC_NAME")
                .requestTimeout("X_REQUEST_TIMEOUT")
                .authnUser("X_AUTHN_USER")
                .replyToHttpUriHeader("X_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader("X_REPLY_TO_HTTP_METHOD")
                .replyToHeader("X_REPLY_TO")
                .httpUriHeader("X_HTTP_URI")
                .httpMethodHeader("X_HTTP_METHOD")
                .httpContentType("X_CONTENT_TYPE")
                .bridged("X_BRIDGED")
                .groupIdHeader("X_GROUP_ID")
                .msgIdHeader("X_MESSAGE_ID")
                .responseCode("X_RESPONSE_CODE")
                .responseBody("X_RESPONSE_BODY")
                .build();
    }

    public static MessageHeaderConfiguration buildConfigWithDefaults(MessageHeaderConfiguration providedConfig) {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(providedConfig.getMsgHeaderPrefix() != null ? providedConfig.getMsgHeaderPrefix() :
                        List.of("X_", "X-"))
                .nfrMessageOrderPathHeader(providedConfig.getNfrMessageOrderPathHeader() != null ?
                        providedConfig.getNfrMessageOrderPathHeader() : "X-PERF-ORDERPATH")
                .nfrMessageHeader1(
                        providedConfig.getNfrMessageHeader1() != null ? providedConfig.getNfrMessageHeader1() :
                                "X_PERF_TEST")
                .nfrMessageHeader2(
                        providedConfig.getNfrMessageHeader2() != null ? providedConfig.getNfrMessageHeader2() :
                                "X-PERF-TEST")
                .proxyUserHeader(providedConfig.getProxyUserHeader() != null ? providedConfig.getProxyUserHeader() :
                        "X-Proxy-User")
                .xForwardedFor(providedConfig.getXForwardedFor() != null ? providedConfig.getXForwardedFor() :
                        "X-FORWARDED-FOR")
                .customTargetClientIdHeader(providedConfig.getCustomTargetClientIdHeader() != null ?
                        providedConfig.getCustomTargetClientIdHeader() : "X_TARGET_CLIENT_ID_HEADER")
                .callbackCodes(providedConfig.getCallbackCodes() != null ? providedConfig.getCallbackCodes() :
                        "X_CALLBACK_CODES")
                .exchangeNameHeader(
                        providedConfig.getExchangeNameHeader() != null ? providedConfig.getExchangeNameHeader() :
                                "X_EXCHANGE_NAME")
                .originalTopicName(
                        providedConfig.getOriginalTopicName() != null ? providedConfig.getOriginalTopicName() :
                                "X_ORIGINAL_TOPIC_NAME")
                .requestTimeout(providedConfig.getRequestTimeout() != null ? providedConfig.getRequestTimeout() :
                        "X_REQUEST_TIMEOUT")
                .authnUser(providedConfig.getAuthnUser() != null ? providedConfig.getAuthnUser() : "X_AUTHN_USER")
                .replyToHttpUriHeader(
                        providedConfig.getReplyToHttpUriHeader() != null ? providedConfig.getReplyToHttpUriHeader() :
                                "X_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader(providedConfig.getReplyToHttpMethodHeader() != null ?
                        providedConfig.getReplyToHttpMethodHeader() : "X_REPLY_TO_HTTP_METHOD")
                .replyToHeader(
                        providedConfig.getReplyToHeader() != null ? providedConfig.getReplyToHeader() : "X_REPLY_TO")
                .httpUriHeader(
                        providedConfig.getHttpUriHeader() != null ? providedConfig.getHttpUriHeader() : "X_HTTP_URI")
                .httpMethodHeader(providedConfig.getHttpMethodHeader() != null ? providedConfig.getHttpMethodHeader() :
                        "X_HTTP_METHOD")
                .httpContentType(providedConfig.getHttpContentType() != null ? providedConfig.getHttpContentType() :
                        "X_CONTENT_TYPE")
                .bridged(providedConfig.getBridged() != null ? providedConfig.getBridged() : "X_BRIDGED")
                .groupIdHeader(
                        providedConfig.getGroupIdHeader() != null ? providedConfig.getGroupIdHeader() : "X_GROUP_ID")
                .msgIdHeader(providedConfig.getMsgIdHeader() != null ? providedConfig.getMsgIdHeader() : "X_MSG_ID")
                .responseCode(
                        providedConfig.getResponseCode() != null ? providedConfig.getResponseCode() : "X_RESPONSE_CODE")
                .responseBody(
                        providedConfig.getResponseBody() != null ? providedConfig.getResponseBody() : "X_RESPONSE_BODY")
                .build();
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

package com.flipkart.varadhi.config;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageHeaderConfigurationTest {

    @Test
    void validConfiguration_success() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.getDefaultMessageHeaders();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void invalidMessageId_fail() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("Y", "Y-"))
                .nfrMessageOrderPathHeader("VARADHI_ORDERPATH")
                .nfrMessageHeader1("VARADHI_TEST")
                .nfrMessageHeader2("VARADHI-TEST")
                .proxyUserHeader("VARADHI-Proxy-User")
                .xForwardedFor("VARADHI-FORWARDED-FOR")
                .customTargetClientIdHeader("VARADHI_TARGET_CLIENT_ID")
                .callbackCodes("VARADHI_CALLBACK_CODES")
                .exchangeNameHeader("VARADHI_EXCHANGE_NAME")
                .originalTopicName("VARADHI_ORIGINAL_TOPIC_NAME")
                .requestTimeout("VARADHI_REQUEST_TIMEOUT")
                .authnUser("VARADHI_AUTHN_USER")
                .replyToHttpUriHeader("VARADHI_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader("VARADHI_REPLY_TO_HTTP_METHOD")
                .replyToHeader("VARADHI_REPLY_TO")
                .httpUriHeader("VARADHI_HTTP_URI")
                .httpMethodHeader("VARADHI_HTTP_METHOD")
                .httpContentType("VARADHI_CONTENT_TYPE")
                .bridged("VARADHI_BRIDGED")
                .groupIdHeader("VARADHI_GROUP_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();

        assertFalse(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void testValidHeaderPrefix() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("VARADHI_","VARADHI-"))
                .nfrMessageOrderPathHeader("VARADHI_ORDERPATH")
                .nfrMessageHeader1("VARADHI_TEST")
                .nfrMessageHeader2("VARADHI-TEST")
                .proxyUserHeader("VARADHI-Proxy-User")
                .xForwardedFor("VARADHI-FORWARDED-FOR")
                .customTargetClientIdHeader("VARADHI_TARGET_CLIENT_ID")
                .callbackCodes("VARADHI_CALLBACK_CODES")
                .exchangeNameHeader("VARADHI_EXCHANGE_NAME")
                .originalTopicName("VARADHI_ORIGINAL_TOPIC_NAME")
                .requestTimeout("VARADHI_REQUEST_TIMEOUT")
                .authnUser("VARADHI_AUTHN_USER")
                .replyToHttpUriHeader("VARADHI_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader("VARADHI_REPLY_TO_HTTP_METHOD")
                .replyToHeader("VARADHI_REPLY_TO")
                .httpUriHeader("VARADHI_HTTP_URI")
                .httpMethodHeader("VARADHI_HTTP_METHOD")
                .httpContentType("VARADHI_CONTENT_TYPE")
                .bridged("VARADHI_BRIDGED")
                .groupIdHeader("VARADHI_GROUP_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void testEmptyHeaderPrefix() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(List.of(""))
                .nfrMessageOrderPathHeader("VARADHI_ORDERPATH")
                .nfrMessageHeader1("VARADHI_TEST")
                .nfrMessageHeader2("VARADHI-TEST")
                .proxyUserHeader("VARADHI-Proxy-User")
                .xForwardedFor("VARADHI-FORWARDED-FOR")
                .customTargetClientIdHeader("VARADHI_TARGET_CLIENT_ID")
                .callbackCodes("VARADHI_CALLBACK_CODES")
                .exchangeNameHeader("VARADHI_EXCHANGE_NAME")
                .originalTopicName("VARADHI_ORIGINAL_TOPIC_NAME")
                .requestTimeout("VARADHI_REQUEST_TIMEOUT")
                .authnUser("VARADHI_AUTHN_USER")
                .replyToHttpUriHeader("VARADHI_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader("VARADHI_REPLY_TO_HTTP_METHOD")
                .replyToHeader("VARADHI_REPLY_TO")
                .httpUriHeader("VARADHI_HTTP_URI")
                .httpMethodHeader("VARADHI_HTTP_METHOD")
                .httpContentType("VARADHI_CONTENT_TYPE")
                .bridged("VARADHI_BRIDGED")
                .groupIdHeader("VARADHI_GROUP_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }
}

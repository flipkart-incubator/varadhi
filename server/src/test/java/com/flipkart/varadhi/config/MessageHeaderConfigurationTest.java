package com.flipkart.varadhi.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MessageHeaderConfigurationTest {

    private static final MessageHeaderConfiguration DEFAULT_CONFIG = getDefaultMessageHeaderConfig();

    private static MessageHeaderConfiguration getDefaultMessageHeaderConfig() {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("VARADHI_", "VARADHI-"))
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
                .msgIdHeader("VARADHI_MSG_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();
    }

    @Test
    void validConfiguration_success() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.buildConfigWithDefaults(new MessageHeaderConfiguration());
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
                .msgIdHeader("VARADHI_MSG_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();

        assertFalse(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @ParameterizedTest
    @CsvSource({
            "'VARADHI_', 'VARADHI-'",
            "'', 'VARADHI-'",
            "'VARADHI_', ''"
    })
    void testHeaderPrefixValidation(String prefix1, String prefix2) throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList(prefix1, prefix2))
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
                .msgIdHeader("VARADHI_MSG_ID")
                .responseCode("VARADHI_RESPONSE_CODE")
                .responseBody("VARADHI_RESPONSE_BODY")
                .build();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void testBuildConfigWithDefaults_allNull() {
        MessageHeaderConfiguration providedConfig = new MessageHeaderConfiguration();
        MessageHeaderConfiguration defaultConfigWithNull = MessageHeaderConfiguration.buildConfigWithDefaults(providedConfig);

        assertEquals(DEFAULT_CONFIG.getProxyUserHeader(), defaultConfigWithNull.getProxyUserHeader());
        assertEquals(DEFAULT_CONFIG.getRequestTimeout(), defaultConfigWithNull.getRequestTimeout());
        assertEquals(DEFAULT_CONFIG.getMsgHeaderPrefix(), defaultConfigWithNull.getMsgHeaderPrefix());
    }
}

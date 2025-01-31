package com.flipkart.varadhi.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MessageHeaderConfigurationTest {

    private static final MessageHeaderConfiguration DEFAULT_CONFIG = getDefaultMessageHeaderConfig();


    private static MessageHeaderConfiguration getDefaultMessageHeaderConfig() {
        return MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("VARADHI_", "VARADHI-"))
                .callbackCodes("VARADHI_CALLBACK_CODES")
                .exchangeNameHeader("VARADHI_EXCHANGE_NAME")
                .originalTopicName("VARADHI_ORIGINAL_TOPIC_NAME")
                .requestTimeout("VARADHI_REQUEST_TIMEOUT")
                .replyToHttpUriHeader("VARADHI_REPLY_TO_HTTP_URI")
                .replyToHttpMethodHeader("VARADHI_REPLY_TO_HTTP_METHOD")
                .replyToHeader("VARADHI_REPLY_TO")
                .httpUriHeader("VARADHI_HTTP_URI")
                .httpMethodHeader("VARADHI_HTTP_METHOD")
                .httpContentType("VARADHI_CONTENT_TYPE")
                .groupIdHeader("VARADHI_GROUP_ID")
                .msgIdHeader("VARADHI_MSG_ID")
                .build();
    }

    @Test
    void shouldThrowIllegalArgumentException_whenInvalidConfiguration() {
        MessageHeaderConfiguration invalidConfig = new MessageHeaderConfiguration();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MessageHeaderConfiguration.buildFromConfig(invalidConfig);
        });
        assertEquals(IllegalArgumentException.class, exception.getClass());
    }

    @ParameterizedTest
    @CsvSource({
            "'VARADHI_', 'VARADHI-', true",
            "'', 'VARADHI-', true",
            "'VARADHI_', '', true",
            "'T_', 'T-', false"
    })
    void testHeaderPrefixValidation(String prefix1, String prefix2, boolean expectedResult) throws IllegalAccessException {
        DEFAULT_CONFIG.setMsgHeaderPrefix(Arrays.asList(prefix1, prefix2));
        assertEquals(expectedResult, MessageHeaderConfiguration.validateHeaderMapping(DEFAULT_CONFIG));
    }

    @Test
    void testBuildConfigWithDefaults_singleNull() {
        //setting it null specifically to test failure
        DEFAULT_CONFIG.setGroupIdHeader(null);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MessageHeaderConfiguration.buildFromConfig(DEFAULT_CONFIG);
        });
        assertEquals(IllegalArgumentException.class, exception.getClass());
    }
}

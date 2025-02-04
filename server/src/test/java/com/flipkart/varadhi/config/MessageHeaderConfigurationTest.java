package com.flipkart.varadhi.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MessageHeaderConfigurationTest {
    private MessageHeaderConfiguration defaultConfig;
    @BeforeEach
    public void beforeTestMethod() {
        defaultConfig = getDefaultMessageHeaderConfig();
    }

    private MessageHeaderConfiguration getDefaultMessageHeaderConfig() {
        return MessageHeaderConfiguration.builder()
                .allowedPrefix(Arrays.asList("VARADHI_", "VARADHI-"))
                .callbackCodes("VARADHI_CALLBACK_CODES")
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
    @ParameterizedTest
    @CsvSource({
            "'VARADHI_', 'VARADHI-', true",
            "'', 'VARADHI-', true",
            "'VARADHI_', '', true",
            "'T_', 'T-', false",
            "'VARADHI_', null, true",
            "'varadhi_', 'VARADHI-', false"  // Case sensitivity
    })
    void testHeaderPrefixValidation(String prefix1, String prefix2, boolean expectedResult) throws IllegalAccessException {
        defaultConfig.setAllowedPrefix(Arrays.asList(prefix1, prefix2));
        if(!expectedResult){
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                MessageHeaderConfiguration.validateHeaderMapping(defaultConfig);
            });
            assertEquals(IllegalArgumentException.class, exception.getClass());
        }else {
            assertEquals(expectedResult, MessageHeaderConfiguration.validateHeaderMapping(defaultConfig));
        }
    }

}

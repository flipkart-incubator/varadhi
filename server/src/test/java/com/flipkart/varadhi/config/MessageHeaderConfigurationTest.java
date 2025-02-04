package com.flipkart.varadhi.config;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class MessageHeaderConfigurationTest {

    private Validator validator;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
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
            "'VARADHI_', 'VARADHI-', true",            // Valid case
            "'', 'VARADHI-', false",                   // Empty prefix
            "'VARADHI_', '', false",                   // Empty second prefix
            "'T_', 'T-', false",                       // Invalid prefix not matching
            "'VARADHI_', null, true",                  // Null second prefix, valid first
            "'varadhi_', 'VARADHI-', false",           // Case sensitivity issue
            "'VARADHI_', 'VARADHI\u00A9', true",       // Unicode characters
    })
    void testHeaderPrefixValidation(String prefix1, String prefix2, boolean expectedResult) {
        MessageHeaderConfiguration config = getDefaultMessageHeaderConfig();
        config.setAllowedPrefix(Arrays.asList(prefix1, prefix2));

        Set<jakarta.validation.ConstraintViolation<MessageHeaderConfiguration>> violations = validator.validate(config);

        if (expectedResult) {
            assertTrue(violations.isEmpty(), "Validation failed but it should have passed.");
        } else {
            assertFalse(violations.isEmpty(), "Validation passed but it should have failed.");
        }
    }
}

package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MessageHeaderConfigurationTest {

    private MessageHeaderConfiguration getDefaultMessageHeaderConfig(String prefix1, String prefix2) {
        return MessageHeaderConfiguration.builder()
                .allowedPrefix(Arrays.asList(prefix1, prefix2))
                .mapping(Map.ofEntries(
                        Map.entry(StandardHeaders.MSG_ID, "X_MESSAGE_ID"),
                        Map.entry(StandardHeaders.GROUP_ID, "X_GROUP_ID"),
                        Map.entry(StandardHeaders.CALLBACK_CODE, "X_CALLBACK_CODES"),
                        Map.entry(StandardHeaders.REQUEST_TIMEOUT, "X_REQUEST_TIMEOUT"),
                        Map.entry(StandardHeaders.REPLY_TO_HTTP_URI, "X_REPLY_TO_HTTP_URI"),
                        Map.entry(StandardHeaders.REPLY_TO_HTTP_METHOD, "X_REPLY_TO_HTTP_METHOD"),
                        Map.entry(StandardHeaders.REPLY_TO, "X_REPLY_TO"),
                        Map.entry(StandardHeaders.HTTP_URI, "X_HTTP_URI"),
                        Map.entry(StandardHeaders.HTTP_METHOD, "X_HTTP_METHOD"),
                        Map.entry(StandardHeaders.CONTENT_TYPE, "X_CONTENT_TYPE"),
                        Map.entry(StandardHeaders.PRODUCE_IDENTITY, "X_PRODUCE_IDENTITY"),
                        Map.entry(StandardHeaders.PRODUCE_REGION, "X_PRODUCE_REGION"),
                        Map.entry(StandardHeaders.PRODUCE_TIMESTAMP, "X_PRODUCE_TIMESTAMP")
                ))
                .maxRequestSize(5 * 1024 * 1024)
                .headerValueSizeMax(100)
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
        MessageHeaderConfiguration config = getDefaultMessageHeaderConfig(prefix1, prefix2);

        Executable validationAction = config::validate;

        if (expectedResult) {
            assertDoesNotThrow(validationAction, "Expected validation to pass but it failed.");
        } else {
            // If expected result is false, it should throw an IllegalArgumentException
            assertThrows(
                    IllegalArgumentException.class, validationAction, "Expected validation to throw an exception.");
        }
    }
}

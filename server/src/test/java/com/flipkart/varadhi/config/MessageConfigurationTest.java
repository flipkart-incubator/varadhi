package com.flipkart.varadhi.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.flipkart.varadhi.entities.StdHeaders;

public class MessageConfigurationTest {
    @ParameterizedTest
    @CsvSource ({
        "'VARADHI_', 'VARADHI-', true",            // Valid case
        "'', 'VARADHI-', false",                   // Empty prefix
        "'VARADHI_', '', false",                   // Empty second prefix
        "'T_', 'T-', false",                       // Invalid prefix not matching
        "'varadhi_', 'VARADHI-', false",            // Case insensitivity
        "'VARADHI_', 'VARADHI\u00A9', true",       // Unicode characters
        "'VARadhi', 'varadhi', false",       // Unicode characters
    })
    void testHeaderPrefixValidation(String prefix1, String prefix2, boolean expectedResult) {
        Runnable validationAction = () -> Assertions.assertNotNull(
            getDefaultMessageHeaderConfig(List.of(prefix1, prefix2))
        );

        if (expectedResult) {
            assertDoesNotThrow(validationAction::run, "Expected validation to pass but it failed.");
        } else {
            assertThrows(
                IllegalArgumentException.class,
                validationAction::run,
                "Expected validation to throw an exception."
            );
        }
    }


    // Utility method to build a default config for testing
    private MessageConfiguration getDefaultMessageHeaderConfig(List<String> prefixes) {
        return new MessageConfiguration(
            new StdHeaders(
                prefixes,
                "VARADHI_MESSAGE_ID",
                "VARADHI_GROUP_ID",
                "VARADHI_CALLBACK_CODES",
                "VARADHI_REQUEST_TIMEOUT",
                "VARADHI_REPLY_TO_HTTP_URI",
                "VARADHI_REPLY_TO_HTTP_METHOD",
                "VARADHI_REPLY_TO",
                "VARADHI_HTTP_URI",
                "VARADHI_HTTP_METHOD",
                "VARADHI_CONTENT_TYPE",
                "VARADHI_PRODUCE_IDENTITY",
                "VARADHI_PRODUCE_REGION",
                "VARADHI_PRODUCE_TIMESTAMP"
            ),
            100,
            2000,
            true
        );
    }
}

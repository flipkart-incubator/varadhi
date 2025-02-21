package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MessageHeaderConfigurationTest {
    @ParameterizedTest
    @CsvSource ({
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
        Runnable validationAction = () -> new MessageHeaderConfiguration(
            config.mapping(),
            List.of(prefix1, prefix2),
            config.headerValueSizeMax(),
            config.maxRequestSize(),
            config.filterNonCompliantHeaders()
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
    private MessageHeaderConfiguration getDefaultMessageHeaderConfig() {
        return new MessageHeaderConfiguration(
            Map.ofEntries(
                Map.entry(StandardHeaders.MSG_ID, "VARADHI_MESSAGE_ID"),
                Map.entry(StandardHeaders.GROUP_ID, "VARADHI_GROUP_ID"),
                Map.entry(StandardHeaders.CALLBACK_CODE, "VARADHI_CALLBACK_CODES"),
                Map.entry(StandardHeaders.REQUEST_TIMEOUT, "VARADHI_REQUEST_TIMEOUT"),
                Map.entry(StandardHeaders.REPLY_TO_HTTP_URI, "VARADHI_REPLY_TO_HTTP_URI"),
                Map.entry(StandardHeaders.REPLY_TO_HTTP_METHOD, "VARADHI_REPLY_TO_HTTP_METHOD"),
                Map.entry(StandardHeaders.REPLY_TO, "VARADHI_REPLY_TO"),
                Map.entry(StandardHeaders.HTTP_URI, "VARADHI_HTTP_URI"),
                Map.entry(StandardHeaders.HTTP_METHOD, "VARADHI_HTTP_METHOD"),
                Map.entry(StandardHeaders.CONTENT_TYPE, "VARADHI_CONTENT_TYPE"),
                Map.entry(StandardHeaders.PRODUCE_IDENTITY, "VARADHI_PRODUCE_IDENTITY"),
                Map.entry(StandardHeaders.PRODUCE_REGION, "VARADHI_PRODUCE_REGION"),
                Map.entry(StandardHeaders.PRODUCE_TIMESTAMP, "VARADHI_PRODUCE_TIMESTAMP")
            ),
            List.of("VARADHI_"),
            100,
            2000,
            true
        );
    }
}

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

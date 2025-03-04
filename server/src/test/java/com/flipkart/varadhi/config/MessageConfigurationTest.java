package com.flipkart.varadhi.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class MessageConfigurationTest {
    @ParameterizedTest
    @CsvSource ({
        "'VARADHI_', 'VARADHI-', true",            // Valid case
        "'', 'VARADHI-', false",                   // Empty prefix
        "'VARADHI_', '', false",                   // Empty second prefix
        "'T_', 'T-', false",                       // Invalid prefix not matching
        "'VARADHI_', null, true",                  // Null second prefix, valid first
        "'varadhi_', 'VARADHI-', true",            // Case insensitivity
        "'VARADHI_', 'VARADHI\u00A9', true",       // Unicode characters
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
        // creating a new instance also validates it.
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

    @Test
    public void testMissingRequiredHeaders() {
        MessageConfiguration msgConfig = MessageHeaderUtils.fetchTestConfiguration();
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        msgConfig.getRequiredHeaders()
                 .stream()
                 .filter(
                     key -> !key.equals(msgConfig.stdHeaders().msgId()) && !key.equals(
                         msgConfig.stdHeaders().produceRegion()
                     )
                 )
                 .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> msgConfig.ensureRequiredHeaders(varadhiHeaders));
    }

    @Test
    public void testCopyVaradhiHeaders() {
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("header1", "h1value1");
        headers.put("Header1", "h1value2");
        headers.put("X_UPPER_CASE", "UPPER_CASE");
        headers.put("x_lower_case", "lower_case");
        headers.put("X_Mixed_Case", "Mixed_Case");
        headers.put("x_multi_value1", "multi_value1_1");
        headers.put("x_multi_value1", "multi_value1_2");
        headers.put("x_Multi_Value2", "multi_value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("xy_header2", "value2");
        headers.put("x__header3", "value3");
        headers.put("x-header4", "value4");
        headers.put("x_Restbus_identity", "value5");
        MessageConfiguration msgConfig = MessageHeaderUtils.fetchTestConfiguration();
        Multimap<String, String> copiedHeaders = msgConfig.filterCompliantHeaders(headers);

        Assertions.assertEquals(10, copiedHeaders.size());
        Assertions.assertEquals("value5", copiedHeaders.get("x_restbus_identity".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("value3", copiedHeaders.get("x__header3".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("Mixed_Case", copiedHeaders.get("x_mixed_case".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("lower_case", copiedHeaders.get("x_lower_case".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("UPPER_CASE", copiedHeaders.get("x_upper_case".toUpperCase()).toArray()[0]);
        Collection<String> multi_value1 = copiedHeaders.get("x_multi_value1".toUpperCase());
        Assertions.assertEquals(2, multi_value1.size());
        Assertions.assertTrue(multi_value1.contains("multi_value1_1"));
        Assertions.assertTrue(multi_value1.contains("multi_value1_2"));
        Collection<String> multi_value2 = copiedHeaders.get("x_multi_value2".toUpperCase());
        Assertions.assertEquals(3, multi_value2.size());
        Assertions.assertTrue(multi_value2.contains("multi_value2_1"));
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));
        multi_value2.remove("multi_Value2_1");
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));

        Assertions.assertTrue(copiedHeaders.get("x-header4".toUpperCase()).isEmpty());
        Assertions.assertTrue(copiedHeaders.get("xy_header2".toUpperCase()).isEmpty());
        Assertions.assertTrue(copiedHeaders.get("header1".toUpperCase()).isEmpty());
        Assertions.assertTrue(copiedHeaders.get("Header1".toUpperCase()).isEmpty());
    }

    @Test
    public void ensureHeaderOrderIsMaintained() {
        MessageConfiguration msgConfig = MessageHeaderUtils.fetchTestConfiguration();
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("x_multi_value1", "multi_value1_2");
        headers.put("x_multi_value1", "multi_value1_1");
        headers.put("x_Multi_Value2", "multi_value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("x_multi_value1", "multi_value1_3");
        Multimap<String, String> copiedHeaders = msgConfig.filterCompliantHeaders(headers);
        String[] values = copiedHeaders.get("x_multi_value1".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value1_2", values[0]);
        Assertions.assertEquals("multi_value1_1", values[1]);
        Assertions.assertEquals("multi_value1_3", values[2]);

        values = copiedHeaders.get("x_multi_value2".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(2, values.length);
        Assertions.assertEquals("multi_value2_1", values[0]);
        Assertions.assertEquals("multi_Value2_1", values[1]);
    }

    @ParameterizedTest
    @ValueSource (booleans = {true, false})
    public void ensureHeadersAreProcessedCorrectly(Boolean filterNonCompliantHeaders) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("x_multi_value1", "multi_value1_2");
        headers.put("x_multi_value1", "multi_value1_1");
        headers.put("x_multi_value1", "multi_value1_3");
        headers.put("x_Multi_Value2", "multi_value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("abc_multi_value1", "multi_value1_3");
        headers.put("xyz_multi_value1", "multi_value1_3");
        headers.put("aaa_multi_value1", "multi_value1_3");
        headers.put("bbb_multi_value1", "multi_value1_3");

        MessageConfiguration msgConfig = MessageHeaderUtils.fetchTestConfiguration(filterNonCompliantHeaders);

        Multimap<String, String> copiedHeaders = msgConfig.filterCompliantHeaders(headers);

        String[] values = copiedHeaders.get("x_multi_value1".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value1_2", values[0]);
        Assertions.assertEquals("multi_value1_1", values[1]);
        Assertions.assertEquals("multi_value1_3", values[2]);

        // Verify that "x_multi_value2" is always included with its 2 values
        values = copiedHeaders.get("x_multi_value2".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(2, values.length);
        Assertions.assertEquals("multi_value2_1", values[0]);
        Assertions.assertEquals("multi_Value2_1", values[1]);

        // Verify that "abc_multi_value1", "xyz_multi_value1", "aaa_multi_value1", and "bbb_multi_value1" are processed based on the filter setting
        List<String> headerKeys = Arrays.asList(
            "abc_multi_value1",
            "xyz_multi_value1",
            "aaa_multi_value1",
            "bbb_multi_value1"
        );

        headerKeys.forEach(headerKey -> {
            int expectedSize = filterNonCompliantHeaders ? 0 : 1;
            Assertions.assertEquals(
                expectedSize,
                copiedHeaders.get(headerKey.toUpperCase()).size(),
                "Header: " + headerKey + " was not processed as expected."
            );
        });
    }
}

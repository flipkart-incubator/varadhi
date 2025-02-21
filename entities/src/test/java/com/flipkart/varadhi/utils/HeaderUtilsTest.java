package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.MessageHeaderUtils;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HeaderUtilsTest {

    @BeforeEach
    public void setup() {
        HeaderUtils.deInitialize();
    }

    @Test
    public void testMissingRequiredHeaders() {
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        HeaderUtils.getRequiredHeaders()
                   .stream()
                   .filter(
                       key -> !key.equals(HeaderUtils.getHeader(MessageHeaders.MSG_ID)) && !key.equals(
                           HeaderUtils.getHeader(MessageHeaders.PRODUCE_REGION)
                       )
                   )
                   .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        IllegalArgumentException ae = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> HeaderUtils.ensureRequiredHeaders(varadhiHeaders)
        );
    }

    @Test
    public void testCopyVaradhiHeaders() {
        Multimap headers = ArrayListMultimap.create();
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
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
        Multimap<String, String> copiedHeaders = HeaderUtils.returnVaradhiRecognizedHeaders(headers);

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
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
        Multimap headers = ArrayListMultimap.create();
        headers.put("x_multi_value1", "multi_value1_2");
        headers.put("x_multi_value1", "multi_value1_1");
        headers.put("x_Multi_Value2", "multi_value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("x_multi_value1", "multi_value1_3");
        Multimap<String, String> copiedHeaders = HeaderUtils.returnVaradhiRecognizedHeaders(headers);
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

        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfigurationWithParams(filterNonCompliantHeaders));

        Multimap<String, String> copiedHeaders = HeaderUtils.returnVaradhiRecognizedHeaders(headers);

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

package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.MessageHeaderUtils;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

public class HeaderUtilsTest {

    @BeforeEach
    public void setup() {
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
    }

    @Test
    public void testAllRequiredHeadersPresent() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        varadhiHeaders.put("X_MESSAGE_ID", "value1");
        varadhiHeaders.put("X_PRODUCE_TIMESTAMP", "value1");
        varadhiHeaders.put("X_PRODUCE_REGION", "value1");
        varadhiHeaders.put("X_PRODUCE_IDENTITY", "value1");
        HeaderUtils.getRequiredHeaders().forEach(
                key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        HeaderUtils.ensureRequiredHeaders(varadhiHeaders);
    }

    @Test
    public void testMissingRequiredHeaders() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        HeaderUtils.getRequiredHeaders().stream().filter(key -> !key.equals(HeaderUtils.getHeader(
                        StandardHeaders.MSG_ID)) && !key.equals(HeaderUtils.getHeader(StandardHeaders.PRODUCE_REGION)))
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

        Multimap<String, String> copiedHeaders = HeaderUtils.copyVaradhiHeaders(headers);

        Assertions.assertEquals(10, copiedHeaders.size());
        Assertions.assertEquals("value5", copiedHeaders.get("x_restbus_identity").toArray()[0]);
        Assertions.assertEquals("value3", copiedHeaders.get("x__header3").toArray()[0]);
        Assertions.assertEquals("Mixed_Case", copiedHeaders.get("x_mixed_case").toArray()[0]);
        Assertions.assertEquals("lower_case", copiedHeaders.get("x_lower_case").toArray()[0]);
        Assertions.assertEquals("UPPER_CASE", copiedHeaders.get("x_upper_case").toArray()[0]);
        Collection<String> multi_value1 = copiedHeaders.get("x_multi_value1");
        Assertions.assertEquals(2, multi_value1.size());
        Assertions.assertTrue(multi_value1.contains("multi_value1_1"));
        Assertions.assertTrue(multi_value1.contains("multi_value1_2"));
        Collection<String> multi_value2 = copiedHeaders.get("x_multi_value2");
        Assertions.assertEquals(3, multi_value2.size());
        Assertions.assertTrue(multi_value2.contains("multi_value2_1"));
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));
        multi_value2.remove("multi_Value2_1");
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));

        Assertions.assertTrue(copiedHeaders.get("X_UPPER_CASE").isEmpty());

        Assertions.assertTrue(copiedHeaders.get("x-header4").isEmpty());
        Assertions.assertTrue(copiedHeaders.get("xy_header2").isEmpty());
        Assertions.assertTrue(copiedHeaders.get("header1").isEmpty());
        Assertions.assertTrue(copiedHeaders.get("Header1").isEmpty());
    }

    @Test
    public void ensureHeaderOrderIsMaintained() {
        Multimap headers = ArrayListMultimap.create();
        headers.put("x_multi_value1", "multi_value1_2");
        headers.put("x_multi_value1", "multi_value1_1");
        headers.put("x_Multi_Value2", "multi_value2_1");
        headers.put("x_multi_value2", "multi_Value2_1");
        headers.put("x_multi_value1", "multi_value1_3");
        Multimap<String, String> copiedHeaders = HeaderUtils.copyVaradhiHeaders(headers);
        String[] values = copiedHeaders.get("x_multi_value1").toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value1_2", values[0]);
        Assertions.assertEquals("multi_value1_1", values[1]);
        Assertions.assertEquals("multi_value1_3", values[2]);

        values = copiedHeaders.get("x_multi_value2").toArray(new String[] {});
        Assertions.assertEquals(2, values.length);
        Assertions.assertEquals("multi_value2_1", values[0]);
        Assertions.assertEquals("multi_Value2_1", values[1]);
    }
}

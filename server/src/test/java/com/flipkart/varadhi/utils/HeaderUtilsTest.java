package com.flipkart.varadhi.utils;

import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;

public class HeaderUtilsTest {

    @Test
    public void testCopyVaradhiHeaders() {
        MultiMap headers = HeadersMultiMap.headers();
        headers.add("header1", "h1value1");
        headers.add("Header1", "h1value2");
        headers.add("X_UPPER_CASE", "UPPER_CASE");
        headers.add("x_lower_case", "lower_case");
        headers.add("X_Mixed_Case", "Mixed_Case");
        headers.add("x_multi_value1", "multi_value1_1");
        headers.add("x_multi_value1", "multi_value1_2");
        headers.add("x_Multi_Value2", "multi_value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("xy_header2", "value2");
        headers.add("x__header3", "value3");
        headers.add("x-header4", "value4");
        headers.add("x_Restbus_identity", "value5");

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
        MultiMap headers = HeadersMultiMap.headers();
        headers.add("x_multi_value1", "multi_value1_2");
        headers.add("x_multi_value1", "multi_value1_1");
        headers.add("x_Multi_Value2", "multi_value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("x_multi_value1", "multi_value1_3");
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

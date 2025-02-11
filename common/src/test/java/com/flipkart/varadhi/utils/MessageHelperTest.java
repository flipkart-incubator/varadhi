package com.flipkart.varadhi.utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.MessageConstants.Headers.REQUIRED_HEADERS;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;
import static com.flipkart.varadhi.entities.StandardHeaders.PRODUCE_REGION;

public class MessageHelperTest {
    @Test
    public void testAllRequiredHeadersPresent() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        REQUIRED_HEADERS.forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        MessageHelper.ensureRequiredHeaders(varadhiHeaders);
    }

    @Test
    public void testMissingRequiredHeaders() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        REQUIRED_HEADERS.stream()
                        .filter(key -> !key.equals(MESSAGE_ID) && !key.equals(PRODUCE_REGION))
                        .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        IllegalArgumentException ae = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MessageHelper.ensureRequiredHeaders(varadhiHeaders)
        );
        Assertions.assertEquals("Missing required header x_restbus_message_id", ae.getMessage());
        varadhiHeaders.put(MESSAGE_ID, "somme random text");
        ae = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MessageHelper.ensureRequiredHeaders(varadhiHeaders)
        );
        Assertions.assertEquals("Missing required header x_restbus_produce_region", ae.getMessage());
    }
}

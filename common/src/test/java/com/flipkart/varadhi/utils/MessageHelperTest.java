package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MessageHelperTest {

    MessageHeaderConfiguration messageHeaderConfiguration;

    @BeforeEach
    public void setup(){
        messageHeaderConfiguration = StandardHeaders.fetchDummyHeaderConfiguration();
    }

    @Test
    public void testAllRequiredHeadersPresent() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        varadhiHeaders.put("X_MESSAGE_ID", "value1");
        varadhiHeaders.put("X_PRODUCE_TIMESTAMP", "value1");
        varadhiHeaders.put("X_PRODUCE_REGION", "value1");
        varadhiHeaders.put("X_PRODUCE_IDENTITY", "value1");
        StandardHeaders.getRequiredHeaders(messageHeaderConfiguration).forEach(
                key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        StandardHeaders.ensureRequiredHeaders(messageHeaderConfiguration, varadhiHeaders);
    }

    @Test
    public void testMissingRequiredHeaders() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        StandardHeaders.getRequiredHeaders(messageHeaderConfiguration).stream().filter(key -> !key.equals(messageHeaderConfiguration.getMsgIdHeader()) && !key.equals(messageHeaderConfiguration.getProduceRegion()))
                .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        IllegalArgumentException ae = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StandardHeaders.ensureRequiredHeaders(messageHeaderConfiguration, varadhiHeaders)
        );
    }
}

package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flipkart.varadhi.MessageConstants.Headers.REQUIRED_HEADERS;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;
import static com.flipkart.varadhi.entities.StandardHeaders.PRODUCE_REGION;

public class MessageHelperTest {

    MessageHeaderConfiguration messageHeaderConfiguration;

    @BeforeEach
    public void setup(){
        messageHeaderConfiguration = MessageHeaderConfiguration.fetchDummyHeaderConfiguration();
    }

    @Test
    public void testAllRequiredHeadersPresent() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        varadhiHeaders.put("X_MESSAGE_ID", "value1");
        varadhiHeaders.put("X_PRODUCE_TIMESTAMP", "value1");
        varadhiHeaders.put("X_PRODUCE_REGION", "value1");
        varadhiHeaders.put("X_PRODUCE_IDENTITY", "value1");
        REQUIRED_HEADERS.forEach(
                key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        MessageHeaderConfiguration.ensureRequiredHeaders(messageHeaderConfiguration, varadhiHeaders);
    }

    @Test
    public void testMissingRequiredHeaders() {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        REQUIRED_HEADERS.stream().filter(key -> !key.equals(MESSAGE_ID) && !key.equals(PRODUCE_REGION))
                .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        IllegalArgumentException ae = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MessageHeaderConfiguration.ensureRequiredHeaders(messageHeaderConfiguration, varadhiHeaders)
        );
        Assertions.assertEquals("Missing required header X_MESSAGE_ID", ae.getMessage());
        varadhiHeaders.put("X_MESSAGE_ID", "somme random text");
        ae = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MessageHeaderConfiguration.ensureRequiredHeaders(messageHeaderConfiguration, varadhiHeaders)
        );
        Assertions.assertEquals("Missing required header X_PRODUCE_TIMESTAMP", ae.getMessage());
    }
}

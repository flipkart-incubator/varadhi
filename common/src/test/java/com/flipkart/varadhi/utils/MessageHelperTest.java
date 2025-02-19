package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
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
        messageHeaderConfiguration = HeaderUtils.fetchDummyHeaderConfiguration();
        HeaderUtils.initialize(messageHeaderConfiguration);
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
        HeaderUtils.getRequiredHeaders().stream().filter(key -> !key.equals(HeaderUtils.mapping.get(
                        StandardHeaders.MSG_ID)) && !key.equals(HeaderUtils.mapping.get(StandardHeaders.PRODUCE_REGION)))
                .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        IllegalArgumentException ae = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.ensureRequiredHeaders(varadhiHeaders)
        );
    }
}

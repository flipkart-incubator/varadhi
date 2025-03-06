package com.flipkart.varadhi.common.utils;

import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.config.MessageHeaderUtils;
import com.flipkart.varadhi.web.WebTestBase;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageRequestValidatorTest extends WebTestBase {
    @Test
    public void testMissingRequiredHeaders() {
        MessageConfiguration msgConfig = MessageHeaderUtils.fetchTestConfiguration();
        //headers
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        MessageRequestValidator.getRequiredHeaders()
                               .stream()
                               .filter(
                                   key -> !key.equals(msgConfig.stdHeaders().msgId()) && !key.equals(
                                       msgConfig.stdHeaders().produceRegion()
                                   )
                               )
                               .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MessageRequestValidator.ensureRequiredHeaders(varadhiHeaders)
        );
    }
}

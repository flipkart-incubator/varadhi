package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.config.MessageHeaderUtils;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.web.WebTestBase;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class MessageRequestValidatorTest extends WebTestBase {
    @Test
    public void testMissingRequiredHeaders() {
        MessageConfiguration msgConfig = MessageHeaderUtils.getTestConfiguration();
        //headers
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        varadhiHeaders.put("Header1", "value1");
        msgConfig.getRequiredHeaders()
                 .stream()
                 .filter(
                     key -> !key.equals(msgConfig.getStdHeaders().msgId()) && !key.equals(
                         msgConfig.getStdHeaders().produceRegion()
                     )
                 )
                 .forEach(key -> varadhiHeaders.put(key, String.format("%s_sometext", key)));
        varadhiHeaders.put("Header2", "value2");
        varadhiHeaders.put("x_header1", "value1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> msgConfig.ensureRequiredHeaders(varadhiHeaders));
    }

    @ParameterizedTest
    @CsvSource ({
        "100000, 4194304, false", // 100000 headers, 4MB payload (failing case)
        "50000, 5242880, false",  // 50000 headers, 5MB payload (failing case)
        "200000, 3145728, false", // 200000 headers, 3MB payload (failing case)
        "10, 1024, true"          // 10 headers, 1KB payload (passing case)
    })
    public void testEnsureHeaderSemanticsAndSize(int headerCount, int payloadSize, boolean shouldPass) {
        MessageConfiguration msgConfig = MessageHeaderUtils.getTestConfiguration();

        Multimap<String, String> requestHeaders = ArrayListMultimap.create();
        requestHeaders.put("X_MESSAGE_ID", "12");
        for (int i = 0; i < headerCount; i++) {
            requestHeaders.put("X_HEADER_" + i, "value_" + i);
        }
        byte[] payload = new byte[payloadSize]; // Payload size from parameters

        Message message = new SimpleMessage(payload, requestHeaders);

        if (shouldPass) {
            Assertions.assertDoesNotThrow(
                () -> MessageRequestValidator.ensureHeaderSemanticsAndSize(msgConfig, message)
            );
        } else {
            IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MessageRequestValidator.ensureHeaderSemanticsAndSize(msgConfig, message)
            );
            Assertions.assertTrue(exception.getMessage().contains("Request size exceeds allowed limit"));
        }
    }
}

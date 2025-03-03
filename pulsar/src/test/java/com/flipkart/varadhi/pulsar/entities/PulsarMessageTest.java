package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerMessage;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.pulsar.PulsarTestBase;
import com.flipkart.varadhi.pulsar.util.PropertyHelper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

class PulsarMessageTest extends PulsarTestBase {
    @BeforeAll
    static void preTest() {
        setUp();
    }

    @Test
    void testPulsarMessagesEqualsProducerMessage() {
        // test request headers
        Multimap<String, String> requestHeaders = ArrayListMultimap.create();
        requestHeaders.put("header1", "value1");
        requestHeaders.put(HeaderUtils.getMessageConfig().getMsgIdHeaderKey(), "msgId");
        requestHeaders.put(HeaderUtils.getMessageConfig().getGroupIdHeaderKey(), "grpId");
        requestHeaders.putAll("header2", List.of("value2", "value3"));

        // now create the producer message
        Message producerMessage = new ProducerMessage("message".getBytes(StandardCharsets.UTF_8), requestHeaders);

        // create produce path message builder
        TypedMessageBuilder<byte[]> messageBuilder = new TypedMessageBuilderImpl<>(null, Schema.BYTES).key("key")
                                                                                                      .value(
                                                                                                          producerMessage.getPayload()
                                                                                                      );
        producerMessage.getHeaders()
                       .asMap()
                       .forEach(
                           (key, values) -> messageBuilder.property(key, PropertyHelper.encodePropertyValues(values))
                       );

        // create pulsar message, which is the message that is consumed by the consumer
        PulsarMessage pulsarMessage = new PulsarMessage(((TypedMessageBuilderImpl<byte[]>)messageBuilder).getMessage());

        // pulsar message and producer message should match
        Assertions.assertEquals(producerMessage.getMessageId(), pulsarMessage.getMessageId());
        Assertions.assertEquals(producerMessage.getGroupId(), producerMessage.getGroupId());

        Assertions.assertEquals(producerMessage.getHeaders().size(), pulsarMessage.getHeaders().size());
        producerMessage.getHeaders().asMap().forEach((key, values) -> {
            Assertions.assertTrue(pulsarMessage.getHeaders().containsKey(key));
            Assertions.assertEquals(values, pulsarMessage.getHeaders().get(key));
        });

    }


}

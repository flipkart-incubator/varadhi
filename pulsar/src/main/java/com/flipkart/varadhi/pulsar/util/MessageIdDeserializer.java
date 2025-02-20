package com.flipkart.varadhi.pulsar.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

public class MessageIdDeserializer extends StdDeserializer<MessageId> {

    public MessageIdDeserializer() {
        this(MessageId.class);
    }

    public MessageIdDeserializer(Class<MessageId> clazz) {
        super(clazz);
    }

    @Override
    public MessageId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        String text = node.get(0).asText();
        return PulsarOffset.messageIdFrom(text);
    }

    @Override
    public MessageId deserializeWithType(
        JsonParser p,
        DeserializationContext deserializationContext,
        TypeDeserializer typeDeserializer
    ) throws IOException {
        return deserialize(p, deserializationContext);
    }
}

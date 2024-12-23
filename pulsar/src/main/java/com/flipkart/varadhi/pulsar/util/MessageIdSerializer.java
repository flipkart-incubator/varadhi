package com.flipkart.varadhi.pulsar.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

public class MessageIdSerializer extends StdSerializer<MessageId> {
    public MessageIdSerializer() {
        this(MessageId.class);
    }

    public MessageIdSerializer(Class<MessageId> clazz) {
        super(clazz);
    }

    @Override
    public void serialize(MessageId messageId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartArray();
        jsonGenerator.writeString("mId:" + messageId.toString());
        jsonGenerator.writeEndArray();
    }

    @Override
    public void serializeWithType(
            MessageId messageId, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer
    ) throws IOException {
        this.serialize(messageId, gen, provider);
    }
}

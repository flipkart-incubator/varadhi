package com.flipkart.varadhi.pulsar.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;

import java.io.IOException;

public class PulsarOffsetSerializer extends StdSerializer<PulsarOffset> {
    public PulsarOffsetSerializer() {
        this(PulsarOffset.class);
    }
    public PulsarOffsetSerializer(Class<PulsarOffset> clazz) {
        super(clazz);
    }

    @Override
    public void serialize(PulsarOffset pulsarOffset, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeString(pulsarOffset.toString());
    }
    @Override
    public void serializeWithType(PulsarOffset pulsarOffset, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        this.serialize(pulsarOffset, gen, provider);
    }
}

package com.flipkart.varadhi.pulsar.util;

import com.beust.jcommander.Strings;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.flipkart.varadhi.pulsar.entities.PulsarOffsets;

import java.io.IOException;

public class PulsarOffsetsSerializer extends StdSerializer<PulsarOffsets>  {
    public PulsarOffsetsSerializer() {
        this(PulsarOffsets.class);
    }
    public PulsarOffsetsSerializer(Class<PulsarOffsets> clazz) {
        super(clazz);
    }

    @Override
    public void serialize(PulsarOffsets pulsarOffsets, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeString(Strings.join(",", pulsarOffsets.getOffsets().stream().map(offset -> offset.toString()).toList()));
    }
    @Override
    public void serializeWithType(PulsarOffsets pulsarOffsets, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        this.serialize(pulsarOffsets, gen, provider);
    }
}

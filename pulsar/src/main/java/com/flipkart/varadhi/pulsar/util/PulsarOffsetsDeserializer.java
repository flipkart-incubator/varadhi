package com.flipkart.varadhi.pulsar.util;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarOffsets;
import org.bouncycastle.util.Strings;

import java.io.IOException;
import java.util.Arrays;

public class PulsarOffsetsDeserializer  extends StdDeserializer<PulsarOffsets> {
    public PulsarOffsetsDeserializer() {
        this(PulsarOffsets.class);
    }

    public PulsarOffsetsDeserializer(Class<PulsarOffsets> clazz) {
        super(clazz);
    }

    @Override
    public PulsarOffsets deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        return new PulsarOffsets(Arrays.stream(Strings.split(jsonParser.getText(), ',')).map(PulsarOffset::fromString).toList());
    }

    @Override
    public PulsarOffsets deserializeWithType(JsonParser p, DeserializationContext deserializationContext, TypeDeserializer typeDeserializer) throws IOException {
        return deserialize(p, deserializationContext);
    }
}

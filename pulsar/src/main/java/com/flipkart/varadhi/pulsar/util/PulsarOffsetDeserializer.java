package com.flipkart.varadhi.pulsar.util;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;

import java.io.IOException;

public class PulsarOffsetDeserializer  extends StdDeserializer<PulsarOffset> {

    public PulsarOffsetDeserializer() {
        this(PulsarOffset.class);
    }

    public PulsarOffsetDeserializer(Class<PulsarOffset> clazz) {
        super(clazz);
    }

    @Override
    public PulsarOffset deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JacksonException {
        return PulsarOffset.fromString(jsonParser.getText());
    }

    @Override
    public PulsarOffset deserializeWithType(JsonParser p, DeserializationContext deserializationContext, TypeDeserializer typeDeserializer) throws IOException, JacksonException {
        return deserialize(p, deserializationContext);
    }
}

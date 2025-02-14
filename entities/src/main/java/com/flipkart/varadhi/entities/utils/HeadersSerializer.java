package com.flipkart.varadhi.entities.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ArrayListMultimap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class HeadersSerializer extends StdSerializer<ArrayListMultimap<String, String>> {
    @SuppressWarnings ("unchecked")
    public HeadersSerializer() {
        this((Class<ArrayListMultimap<String, String>>)(Class<?>)ArrayListMultimap.class);
    }

    public HeadersSerializer(Class<ArrayListMultimap<String, String>> clazz) {
        super(clazz);
    }

    @Override
    public void serialize(
        ArrayListMultimap<String, String> headers,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider
    ) throws IOException {
        Map<String, Collection<String>> map = headers.asMap();
        jsonGenerator.writeObject(map);
    }

    @Override
    public void serializeWithType(
        ArrayListMultimap<String, String> map,
        JsonGenerator gen,
        SerializerProvider provider,
        TypeSerializer typeSer
    ) throws IOException {
        this.serialize(map, gen, provider);
    }
}

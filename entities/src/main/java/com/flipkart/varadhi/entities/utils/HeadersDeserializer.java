package com.flipkart.varadhi.entities.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.common.collect.ArrayListMultimap;

import java.io.IOException;
import java.util.Iterator;


public class HeadersDeserializer extends StdDeserializer<ArrayListMultimap<String, String>> {
    @SuppressWarnings("unchecked")
    public HeadersDeserializer() {
        this((Class<ArrayListMultimap<String, String>>) (Class<?>) ArrayListMultimap.class);
    }

    public HeadersDeserializer(Class<ArrayListMultimap<String, String>> clazz) {
        super(clazz);
    }

    @Override
    public ArrayListMultimap<String, String> deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext
    )
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        ArrayListMultimap<String, String> multiMap = ArrayListMultimap.create();

        // Iterate through the fields of the JSON node
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode valuesNode = node.get(key);
            if (valuesNode.isArray()) {
                // Add each value in the array to the multiMap
                for (JsonNode valueNode : valuesNode) {
                    multiMap.put(key, valueNode.asText());
                }
            }
        }
        return multiMap;
    }

    @Override
    public ArrayListMultimap<String, String> deserializeWithType(
            JsonParser p, DeserializationContext deserializationContext, TypeDeserializer typeDeserializer
    ) throws IOException {
        return deserialize(p, deserializationContext);
    }
}

package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Deserializes {@link HeaderSpec} from either a string (value only, requiredBy = Both)
 * or an object {"value": "...", "requiredBy": "Subscription"|"Queue"|"Both"|"Callback"}.
 */
public class HeaderSpecDeserializer extends StdDeserializer<HeaderSpec> {

    public HeaderSpecDeserializer() {
        super(HeaderSpec.class);
    }

    @Override
    public HeaderSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = ctxt.getParser().getCodec().readTree(p);
        if (node.isTextual()) {
            return HeaderSpec.fromString(node.asText());
        }
        if (node.isObject()) {
            String value = node.has("value") ? node.get("value").asText() : null;
            RequiredBy requiredBy = RequiredBy.Both;
            if (node.has("requiredBy")) {
                String raw = node.get("requiredBy").asText();
                try {
                    requiredBy = RequiredBy.valueOf(raw);
                } catch (IllegalArgumentException e) {
                    if (!raw.isEmpty()) {
                        String capped = raw.substring(0, 1).toUpperCase() + raw.substring(1).toLowerCase();
                        requiredBy = RequiredBy.valueOf(capped);
                    }
                }
            }
            return new HeaderSpec(value != null ? value : "", requiredBy);
        }
        throw new IllegalArgumentException("HeaderSpec must be a string or an object with value and requiredBy");
    }
}

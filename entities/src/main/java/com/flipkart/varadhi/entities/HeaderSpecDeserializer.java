package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Deserializes {@link HeaderSpec} from either a string (value only, requiredBy =
 * {@link RequiredBy#mandatoryHeaderRequiredForProduce()}) or an object with {@code value} and optional {@code requiredBy}.
 * <p>
 * {@code requiredBy} accepts {@link RequiredBy} enum names ({@code Queue}, {@code Both}) plus alias
 * {@code mandatoryHeaderRequiredForProduce} (any case) for {@link RequiredBy#Both}.
 */
public class HeaderSpecDeserializer extends StdDeserializer<HeaderSpec> {

    public HeaderSpecDeserializer() {
        super(HeaderSpec.class);
    }

    static RequiredBy parseRequiredBy(String raw) {
        if (raw == null || raw.isEmpty()) {
            return RequiredBy.mandatoryHeaderRequiredForProduce();
        }
        if ("mandatoryHeaderRequiredForProduce".equalsIgnoreCase(raw)) {
            return RequiredBy.mandatoryHeaderRequiredForProduce();
        }
        try {
            return RequiredBy.valueOf(raw);
        } catch (IllegalArgumentException e) {
            String capped = raw.substring(0, 1).toUpperCase() + raw.substring(1).toLowerCase();
            return RequiredBy.valueOf(capped);
        }
    }

    @Override
    public HeaderSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = ctxt.getParser().getCodec().readTree(p);
        if (node.isTextual()) {
            return HeaderSpec.fromString(node.asText());
        }
        if (node.isObject()) {
            String value = node.has("value") ? node.get("value").asText() : null;
            RequiredBy requiredBy = RequiredBy.mandatoryHeaderRequiredForProduce();
            if (node.has("requiredBy")) {
                requiredBy = parseRequiredBy(node.get("requiredBy").asText());
            }
            return new HeaderSpec(value != null ? value : "", requiredBy);
        }
        throw new IllegalArgumentException("HeaderSpec must be a string or an object with value and requiredBy");
    }
}

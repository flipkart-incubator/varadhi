package com.flipkart.varadhi.authn;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class AuthenticationMechanismDeserializer extends JsonDeserializer<AuthenticationMechanism> {

    @Override
    public AuthenticationMechanism deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {

        validateJsonParser(jsonParser);
        try {
            String value = jsonParser.getText().toUpperCase();
            return AuthenticationMechanism.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid authentication mechanism: " + jsonParser.getText(), e);
        }
    }

    private void validateJsonParser(JsonParser jsonParser) throws IOException {
        if (jsonParser == null || jsonParser.getText() == null) {
            throw new IOException("JsonParser or authentication mechanism value cannot be null");
        }

    }
}

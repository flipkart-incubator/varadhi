package com.flipkart.varadhi.authn;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class AuthenticationMechanismDeserializer extends JsonDeserializer<AuthenticationMechanism> {

    @Override
    public AuthenticationMechanism deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
        String value = jsonParser.getText().toUpperCase();
        return AuthenticationMechanism.valueOf(value);
    }
}

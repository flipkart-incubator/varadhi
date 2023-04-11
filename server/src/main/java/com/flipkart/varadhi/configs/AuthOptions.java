package com.flipkart.varadhi.configs;

import com.flipkart.varadhi.exceptions.InvalidConfigException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import lombok.Data;

import java.util.HashMap;

public class AuthOptions extends HashMap<AuthOptions.Mechanism, Object> {

    public enum Mechanism {
        jwt
    }

    public Mechanism getMechanism() {
        validate();
        return keySet().iterator().next();
    }

    void validate() throws InvalidConfigException {
        if (size() != 1) {
            throw new InvalidConfigException("Only 1 auth config can be provided");
        }
    }

    public <T> T asConfig(Class<T> configClass) {
        validate();
        Object json = values().iterator().next();

        try {
            return JsonObject.mapFrom(json).mapTo(configClass);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigException(e);
        }
    }

    @Data
    public static class JWTConfig {
        private String jwksUrl;
        private JWTOptions options;
    }
}

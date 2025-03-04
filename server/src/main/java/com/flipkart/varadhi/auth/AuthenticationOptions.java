package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import lombok.Data;

import java.util.HashMap;

public class AuthenticationOptions extends HashMap<AuthenticationOptions.Mechanism, Object> {

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

    public enum Mechanism {
        jwt, user_header
    }


    @Data
    public static class JWTConfig {
        private String jwksUrl;
        private JWTOptions options;
    }
}

package com.flipkart.varadhi.auth;

import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.Getter;

import java.util.List;

@Data
public class AuthorizationOptions {

    private List<String> superUsers;

    /**
     * Fully qualified package path to the class implementing AuthorizationProvider interface.<br/>
     * If {@link AuthorizationOptions#useDefaultProvider} is {@code true}, then this configuration is ignored.
     */
    private String providerClassName;

    private JsonObject providerOptions;
    private String configFile;

    /**
     * If specified as true, then the default implementation via {@link com.flipkart.varadhi.auth.DefaultAuthorizationProvider} is used.<br/>
     * {@link AuthorizationOptions#providerClassName} value is ignored.
     */
    private Boolean useDefaultProvider;
}

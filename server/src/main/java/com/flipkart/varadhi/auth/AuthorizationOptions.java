package com.flipkart.varadhi.auth;

import io.vertx.core.json.JsonObject;
import lombok.Getter;

import java.util.List;

@Getter
public class AuthorizationOptions {

    private List<String> superUsers;

    private String providerClassName;

    private JsonObject providerOptions;
}

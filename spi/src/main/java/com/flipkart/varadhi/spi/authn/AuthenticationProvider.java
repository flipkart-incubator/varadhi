package com.flipkart.varadhi.spi.authn;

import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import io.vertx.core.Future;

import java.net.URI;
import java.util.Map;

public interface AuthenticationProvider {
    Future<Boolean> init(ConfigFileResolver resolver, AuthenticationOptions authenticationOptions);
    Future<UserContext> authenticate(URI uri, Map<String,String> params, Map<String,String> headers, Map<String, Object> context);
}

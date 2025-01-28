package com.flipkart.varadhi.spi.authn;

import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.core.Future;

import java.net.URI;
import java.util.Map;

public interface VaradhiAuthenticationProvider {
    Future<Boolean> init(AuthenticationOptions authenticationOptions);
    Future<UserContext> handle(URI uri, Map<String,String> params, Map<String,String> headers, Map<String, Object> context);
}
